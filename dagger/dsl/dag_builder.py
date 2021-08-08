"""Build DAGs through an imperative domain-specific language."""

import inspect
from contextvars import copy_context
from itertools import groupby
from typing import Callable, List, Mapping, Optional, Union

from dagger.dag import DAG, DAGOutput, Node
from dagger.dsl.context import node_invocations
from dagger.dsl.errors import POTENTIAL_BUG_MESSAGE
from dagger.dsl.node_invocations import NodeInvocation, NodeType, SupportedNodeInput
from dagger.dsl.node_outputs import (
    NodeOutputKeyUsage,
    NodeOutputPropertyUsage,
    NodeOutputReference,
    NodeOutputUsage,
)
from dagger.dsl.parameter_usage import ParameterUsage
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromKey, FromProperty, FromReturnValue
from dagger.task import SupportedOutputs as SupportedTaskOutputs
from dagger.task import Task


class DAGBuilder:
    """
    Builds a DAG using the imperative DSL.

    When an instance of this class is invoked, it initializes a new context
    and calls the supplied build function.

    All the nodes, inputs and outputs that were used during the construction of the DAG
    are captured by the context and then rendered as a DAG data structure.
    """

    def __init__(
        self,
        build_func: Callable,
        inputs: Optional[Mapping[str, SupportedNodeInput]] = None,
        parent_node_names_by_id: Optional[Mapping[str, str]] = None,
    ):
        self._build_func = build_func
        self._parameters = {
            param_name: ParameterUsage(name=param_name)
            for param_name in inspect.signature(self._build_func).parameters
        }
        self._inputs_from_parent = inputs
        self._parent_node_names_by_id = parent_node_names_by_id or {}

    def build(self) -> DAG:
        """
        Invoke the builder function and return the DAG data structure it defines.

        It performs the following steps:


        Create a clean context
        ----------------------
        We initialize a new context and invoke the build function inside of it.
        All node, input and output invocations are thus isolated from other
        build functions invoked before, during or after this one.


        Build the DAG inputs
        --------------------
        If this DAG is being invoked within the context of another DAG (that is,
        we are composing several DAGs together), we use the inputs supplied by
        the parent. Otherwise, we use the inputs defined in the function's signature.


        Execute the build function in the new context
        ---------------------------------------------
        We run the build function to record all node invocations.


        Capture DAG outputs and consume them
        ---------------------------------------
        If the build function has a return value, we capture it and add it
        to the set of DAG outputs.


        Build the Nodes based on the recorded NodeInvocations
        -----------------------------------------------------
        We build all of the DAG's nodes. If a node is a DAG, we invoke the builder
        recursively.
        """
        ctx = copy_context()
        dag_output = ctx.run(self._build_func, **self._parameters)

        node_names_by_id = DAGBuilder._translate_invocation_ids_into_readable_names(
            ctx[node_invocations]
        )

        inputs = self._inputs_from_parent or self._parameters
        dag_inputs = {
            input_name: DAGBuilder._build_node_input(
                input_type,
                node_names_by_id=self._parent_node_names_by_id,
            )
            for input_name, input_type in inputs.items()
        }

        dag_outputs = DAGBuilder._build_dag_outputs(
            dag_output,
            node_names_by_id=node_names_by_id,
        )

        dag_nodes = {
            node_names_by_id[node_invocation.id]: DAGBuilder._build_node(
                node_invocation, node_names_by_id=node_names_by_id
            )
            for node_invocation in ctx[node_invocations]
        }

        return DAG(
            inputs=dag_inputs,
            outputs=dag_outputs,
            nodes=dag_nodes,
        )

    @staticmethod
    def _build_dag_outputs(
        dag_output: Union[
            None,
            NodeOutputReference,
            Mapping[str, NodeOutputReference],
        ],
        node_names_by_id: Mapping[str, str],
    ) -> Mapping[str, DAGOutput]:
        """Build the outputs of a DAG."""
        outputs_by_name: Mapping[str, NodeOutputReference] = {}

        if isinstance(dag_output, NodeOutputReference):
            outputs_by_name = {"return_value": dag_output}
        elif isinstance(dag_output, Mapping):
            outputs_by_name = dag_output
        elif dag_output is not None:
            raise TypeError(
                f"This DAG returned a value of type {type(dag_output).__name__}. Functions decorated with `dsl.DAG` may only return two types of values: The output of another node or a mapping of [str, the output of another node]"
            )

        # Explicitly mark direct outputs from other nodes as consumed.

        # This is only necessary for outputs of type NodeOutputUsage.
        # See the documentation of the `.consume()` function to understand why.
        for output_ref in outputs_by_name.values():
            if isinstance(output_ref, NodeOutputUsage):
                output_ref.consume()

        return {
            output_name: DAGOutput(
                node=node_names_by_id[output_ref.invocation_id],
                output=output_ref.output_name,
            )
            for output_name, output_ref in outputs_by_name.items()
        }

    @staticmethod
    def _translate_invocation_ids_into_readable_names(
        node_invocations: List[NodeInvocation],
    ) -> Mapping[str, str]:
        """
        Return a map translating invocation ids (UUIDv4) into unique human-readable names.

        We use UUIDv4 as invocation ids to guarantee uniqueness when recording input/output usage.
        In the context of a DAG definition, we already know all the nodes that have been invoked.
        Therefore, we can use this information to generate unique names that are more meaningful,
        based on the name of the function and a sequence number identifying in which position
        they were invoked.

        For example, the following DAG definition:

        ```
        @dsl.task
        def f():
            pass

        @dsl.task
        def g():
            pass

        @dsl.DAG
        def dag():
            f()
            g()
            g()
        ```

        Would generate the following mapping:

        ```
        {
            "f": "<uuid>",
            "g-1": "<uuid>",
            "g-2": "<uuid>",
        }
        ```

        The sequence number is only appended if the same task is invoked multiple times.
        """
        node_names_by_id = {}
        for node_name, group in groupby(
            node_invocations, key=lambda invocation: invocation.name
        ):
            nodes_with_the_same_name = list(group)

            if len(nodes_with_the_same_name) == 1:
                node_names_by_id[nodes_with_the_same_name[0].id] = node_name
            else:
                for i in range(0, len(nodes_with_the_same_name)):
                    node_names_by_id[
                        nodes_with_the_same_name[i].id
                    ] = f"{node_name}-{i+1}"

        return node_names_by_id

    @staticmethod
    def _build_node_input(
        input_type: SupportedNodeInput,
        node_names_by_id: Mapping[str, str],
    ) -> Union[FromParam, FromNodeOutput]:
        """
        Return an input for a Node, based on the input_type recorded by the DSL.

        If an input references the output of another node by name, it will reference it
        by its unique invocation ID. This method ensures all those references are translated
        into the unique, human-readable name that was generated by this builder.
        """
        if isinstance(input_type, ParameterUsage):
            return FromParam(name=input_type.name)
        elif isinstance(input_type, NodeOutputReference):
            return FromNodeOutput(
                node=node_names_by_id[input_type.invocation_id],
                output=input_type.output_name,
            )
        else:
            raise NotImplementedError(
                f"The DSL is not compatible with inputs of type '{type(input_type).__name__}'. {POTENTIAL_BUG_MESSAGE}"
            )

    @staticmethod
    def _build_task_output(
        node_output_reference: NodeOutputReference,
    ) -> SupportedTaskOutputs:
        if isinstance(node_output_reference, NodeOutputKeyUsage):
            return FromKey(node_output_reference.key_name)
        elif isinstance(node_output_reference, NodeOutputPropertyUsage):
            return FromProperty(node_output_reference.property_name)
        elif isinstance(node_output_reference, NodeOutputUsage):
            return FromReturnValue()
        else:
            raise NotImplementedError(
                f"The DSL is not compatible with node outputs of type '{type(node_output_reference).__name__}'. {POTENTIAL_BUG_MESSAGE}"
            )

    @staticmethod
    def _build_node(
        node_invocation: NodeInvocation,
        node_names_by_id: Mapping[str, str],
    ) -> Node:
        """Build a node (a task or DAG) based on the data collected during its invocation."""
        if node_invocation.node_type == NodeType.TASK:
            return Task(
                node_invocation.func,
                inputs={
                    input_name: DAGBuilder._build_node_input(
                        input_type, node_names_by_id=node_names_by_id
                    )
                    for input_name, input_type in node_invocation.inputs.items()
                },
                outputs={
                    ref.output_name: DAGBuilder._build_task_output(ref)
                    for ref in node_invocation.output.references
                },
            )
        elif node_invocation.node_type == NodeType.DAG:
            return DAGBuilder(
                build_func=node_invocation.func,
                inputs=node_invocation.inputs,
                parent_node_names_by_id=node_names_by_id,
            ).build()
        else:
            raise NotImplementedError(
                f"The DSL is not compatible with node invocations of type '{node_invocation.node_type}'. {POTENTIAL_BUG_MESSAGE}"
            )
