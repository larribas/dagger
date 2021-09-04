"""Define the data structure for a DAG and validate all its components upon initialization."""
import re
import warnings
from typing import Any, List, Mapping, Optional, Set, Union
from typing import get_args as get_type_args

from dagger.dag.topological_sort import topological_sort
from dagger.data_structures import FrozenMapping
from dagger.input import FromNodeOutput, FromParam
from dagger.input import validate_name as validate_input_name
from dagger.output import validate_name as validate_output_name
from dagger.serializer import SerializationError
from dagger.task import SupportedInputs as SupportedTaskInputs
from dagger.task import Task

VALID_NAME_REGEX = r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$"
VALID_NAME = re.compile(VALID_NAME_REGEX)

Node = Union[
    Task,
    "DAG",
]

SupportedInputs = Union[
    FromParam,
    FromNodeOutput,
]

SupportedOutputs = Union[
    FromNodeOutput,
]


class DAG:
    """
    Data Structure that represents a DAG for later execution.

    DAGs are made up of:
    - A set of nodes, connected to each other through their inputs/outputs
    - A set of inputs to the DAG
    - A set of outputs from the DAG
    """

    def __init__(
        self,
        nodes: Mapping[str, Node],
        inputs: Mapping[str, SupportedInputs] = None,
        outputs: Mapping[str, SupportedOutputs] = None,
        runtime_options: Mapping[str, Any] = None,
        partition_by_input: Optional[str] = None,
    ):
        """
        Validate and initialize a DAG.

        Parameters
        ----------
        nodes
            A mapping from node names to nodes.
            Only certain types are allowed as nodes.

        inputs
            A mapping from input names to DAG inputs.
            Only certain types are allowed as inputs.

        outputs
            A mapping from output names to DAG outputs.
            Outputs must come from the output of a node within the DAG.

        runtime_options
            A list of options to supply to all runtimes.
            This allows you to take full advantage of the features of each runtime. For instance, you can use it to manipulate node affinities and tolerations in Kubernetes.
            Check the documentation of each runtime to see potential options.

        partition_by_input
            If specified, it signals the task should be run as many times as partitions in the specified input.
            Each of the executions will only receive one of the partitions of that input.

        Returns
        -------
        A valid, immutable representation of a DAG


        Raises
        ------
        TypeError
            If any of the supplied parameters has an invalid type. Types should match the ones defined in the method signature.

        ValueError
            If any of the names are invalid, or any of the inputs/outputs point to non-existing nodes.

        CyclicDependencyError
            If the nodes contain cyclic dependencies to one another.
        """
        inputs = FrozenMapping(
            inputs or {},
            error_message="You may not mutate the inputs of a DAG after it has been initialized. We do this to guarantee the structures you build with dagger remain valid and consistent.",
        )
        outputs = FrozenMapping(
            outputs or {},
            error_message="You may not mutate the outputs of a DAG after it has been initialized. We do this to guarantee the structures you build with dagger remain valid and consistent.",
        )
        nodes = FrozenMapping(
            nodes,
            error_message="You may not mutate the nodes of a DAG after it has been initialized. We do this to guarantee the structures you build with dagger remain valid and consistent.",
        )

        _validate_nodes_are_not_empty(nodes)

        for node_name in nodes:
            _validate_node_name(node_name)

        for input_name in inputs:
            validate_input_name(input_name)
            _validate_input_is_supported(input_name, inputs[input_name])

        for output_name in outputs:
            validate_output_name(output_name)

        _validate_node_input_dependencies(nodes, inputs)
        _validate_outputs(nodes, outputs)

        if partition_by_input and partition_by_input not in inputs:
            raise ValueError(
                f"This node is partitioned by '{partition_by_input}'. However, '{partition_by_input}' is not an input of the node. The available inputs are {sorted(list(inputs))}."
            )

        self._nodes = nodes
        self._inputs = inputs
        self._outputs = outputs
        self._runtime_options = runtime_options or {}
        self._partition_by_input = partition_by_input
        self._node_execution_order = topological_sort(
            {
                node_name: _node_dependencies(nodes[node_name].inputs)
                for node_name in nodes
            }
        )

    @property
    def nodes(self) -> Mapping[str, Node]:
        """Get the nodes that compose the DAG."""
        return self._nodes

    @property
    def inputs(self) -> Mapping[str, SupportedInputs]:
        """Get the inputs the DAG expects."""
        return self._inputs

    @property
    def outputs(self) -> Mapping[str, SupportedOutputs]:
        """Get the outputs the DAG produces."""
        return self._outputs

    @property
    def runtime_options(self) -> Mapping[str, Any]:
        """Get the specified runtime options."""
        return self._runtime_options

    @property
    def partition_by_input(self) -> Optional[str]:
        """Return the input this task should be partitioned by, if any."""
        return self._partition_by_input

    @property
    def node_execution_order(self) -> List[Set[str]]:
        """
        Get a list of nodes to execute in order, respecting dependencies between nodes.

        Returns
        -------
        A list of sets
            Each set contains the names of the nodes that can be executed concurrently.
            The list represents the right order of execution for each of the sets.
        """
        return self._node_execution_order

    def __repr__(self) -> str:
        """Return a human-readable representation of the DAG."""
        return f"DAG(inputs={self._inputs}, outputs={self._outputs}, runtime_options={self._runtime_options}, partition_by_input={self._partition_by_input}, nodes={self._nodes})"

    def __eq__(self, obj) -> bool:
        """Return true if the two DAGs are equivalent to each other."""
        return (
            self._nodes == obj._nodes
            and self._inputs == obj._inputs
            and self._outputs == obj._outputs
            and self._runtime_options == obj._runtime_options
        )


def validate_parameters(
    inputs: Mapping[str, SupportedInputs],
    params: Mapping[str, Any],
):
    """
    Validate a series of parameters against the inputs of a DAG.

    Parameters
    ----------
    inputs
        A mapping of input names to inputs.

    params
        A mapping of input names to parameters or input values.
        Input values must be passed in their serialized representation.

    Raises
    ------
    ValueError
        If the set of parameters does not contain all the required inputs.

    SerializationError
        If the value provided for a parameter is not compatible with the serializer defined for that input.
    """
    missing_params = inputs.keys() - params.keys()
    if missing_params:
        raise ValueError(
            f"The parameters supplied to this DAG were supposed to contain the following parameters: {sorted(list(inputs))}. However, only the following parameters were actually supplied: {sorted(list(params))}. We are missing: {sorted(list(missing_params))}."
        )

    superfluous_params = params.keys() - inputs.keys()
    if superfluous_params:
        warnings.warn(
            f"The following parameters were supplied to this DAG, but are not necessary: {sorted(list(superfluous_params))}"
        )

    for input_name in inputs:
        try:
            inputs[input_name].serializer.serialize(params[input_name])
        except SerializationError as e:
            raise SerializationError(
                f"The value supplied for input '{input_name}' is not compatible with the serializer defined for that input ({inputs[input_name].serializer}): {e}"
            )


def _validate_node_name(name: str):
    if not VALID_NAME.match(name):
        raise ValueError(
            f"'{name}' is not a valid name for a node. Node names must comply with the regex {VALID_NAME_REGEX}"
        )


def _node_dependencies(node_inputs: Mapping[str, SupportedTaskInputs]) -> Set[str]:
    return {
        input_from_node_output.node
        for input_from_node_output in node_inputs.values()
        if isinstance(input_from_node_output, FromNodeOutput)
    }


def _validate_outputs(
    dag_nodes: Mapping[str, Node],
    dag_outputs: Mapping[str, FromNodeOutput],
):
    for output_name, output_type in dag_outputs.items():
        if output_type.node not in dag_nodes:
            raise ValueError(
                f"Output '{output_name}' depends on the output of a node named '{output_type.node}'. However, the DAG does not contain any node with such a name. These are the nodes contained by the DAG: {list(dag_nodes)}"
            )

        referenced_node = dag_nodes[output_type.node]
        if output_type.output not in referenced_node.outputs:
            raise ValueError(
                f"Output '{output_name}' depends on the output '{output_type.output}' of another node named '{output_type.node}'. However, node '{output_type.node}' does not declare any output with such a name. These are the outputs defined by the node: {list(referenced_node.outputs)}"
            )

        if referenced_node.partition_by_input:
            raise ValueError(
                f"Output '{output_name}' comes from node '{output_type.node}', which is partitioned. This is not a valid map-reduce pattern in dagger. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
            )

        if len(set(dag_outputs.values())) != len(dag_outputs):
            raise ValueError(
                "Multiple DAG outputs depend on the same node output. This is not a valid pattern in dagger due to the ambiguity and potential problems it may cause."
            )


def _validate_nodes_are_not_empty(nodes: Mapping[str, Node]):
    if len(nodes) == 0:
        raise ValueError("A DAG needs to contain at least one node")


def _validate_node_input_dependencies(
    dag_nodes: Mapping[str, Node],
    dag_inputs: Mapping[str, SupportedInputs],
):
    for node_name, node in dag_nodes.items():
        for input_name, input_type in node.inputs.items():
            _validate_node_input_dependency(
                node_name=node_name,
                dag_nodes=dag_nodes,
                dag_inputs=dag_inputs,
                input_name=input_name,
                input_type=input_type,
            )


def _validate_node_input_dependency(
    node_name: str,
    dag_nodes: Mapping[str, Node],
    dag_inputs: Mapping[str, SupportedInputs],
    input_name: str,
    input_type: Union[FromParam, FromNodeOutput],
):
    try:
        if isinstance(input_type, FromParam):
            _validate_input_from_param(
                input_name=input_name,
                input_type=input_type,
                dag_inputs=dag_inputs,
            )
        else:
            _validate_input_from_node_output(
                node_name=node_name,
                input_type=input_type,
                dag_nodes=dag_nodes,
            )
    except (TypeError, ValueError) as e:
        raise e.__class__(
            f"Error validating input '{input_name}' of node '{node_name}': {str(e)}"
        )


def _validate_input_from_param(
    input_name: str,
    input_type: FromParam,
    dag_inputs: Mapping[str, SupportedInputs],
):
    # If the param name has not been overridden, we assume it has the same name as the input
    name = input_type.name or input_name

    if name not in dag_inputs:
        raise ValueError(
            f"This input depends on a parameter named '{name}' being injected into the DAG. However, the DAG does not have any parameter with such a name. These are the parameters the DAG receives: {sorted(list(dag_inputs))}"
        )

    if input_type.serializer != dag_inputs[name].serializer:
        raise ValueError(
            f"This input is serialized {input_type.serializer}. However, the input it references is serialized {dag_inputs[name].serializer}."
        )


def _validate_input_from_node_output(
    node_name: str,
    input_type: FromNodeOutput,
    dag_nodes: Mapping[str, Node],
):
    if input_type.node not in dag_nodes:
        raise ValueError(
            f"This input depends on the output of another node named '{input_type.node}'. However, the DAG does not define any node with such a name. These are the nodes contained by the DAG: {list(dag_nodes)}"
        )

    referenced_node_outputs = dag_nodes[input_type.node].outputs
    if input_type.output not in referenced_node_outputs:
        raise ValueError(
            f"This input depends on the output '{input_type.output}' of another node named '{input_type.node}'. However, node '{input_type.node}' does not declare any output with such a name. These are the outputs defined by the node: {list(referenced_node_outputs)}"
        )

    if input_type.serializer != referenced_node_outputs[input_type.output].serializer:
        raise ValueError(
            f"This input is serialized {input_type.serializer}. However, the output it references is serialized {referenced_node_outputs[input_type.output].serializer}."
        )

    if (
        dag_nodes[node_name].partition_by_input
        and dag_nodes[input_type.node].partition_by_input
    ):
        raise ValueError(
            "This node is partitioned by an input that comes from the output of another partitioned node. This is not a valid map-reduce pattern in dagger. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
        )


def _validate_input_is_supported(input_name: str, input):
    if not _is_type_supported(input, SupportedInputs):
        raise TypeError(
            f"Input '{input_name}' is of type '{type(input).__name__}'. However, DAGs only support the following types of inputs: {[t.__name__ for t in get_type_args(SupportedInputs)]}"
        )


def _is_type_supported(obj, union):
    return any(
        [isinstance(obj, supported_type) for supported_type in get_type_args(union)]
    )
