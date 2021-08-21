"""Run a DAG in memory."""
import itertools
from typing import Any, Dict, List, Mapping, Optional, Union

from dagger.dag import DAG, Node, validate_parameters
from dagger.input import FromNodeOutput, FromParam
from dagger.runtime.local.task import _invoke_task
from dagger.runtime.local.types import (
    NodeExecutions,
    NodeOutput,
    NodeOutputs,
    NodeParams,
    Partitioned,
)
from dagger.serializer import SerializationError, Serializer
from dagger.task import Task


def invoke(
    node: Node,
    params: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, NodeOutput]:
    """
    Invoke a node with a series of parameters.

    Parameters
    ----------
    node
        Node to execute

    params
        Inputs to the task, indexed by input/parameter name.


    Returns
    -------
    Serialized outputs of the task, indexed by output name.


    Raises
    ------
    ValueError
        When any required parameters are missing

    TypeError
        When any of the outputs cannot be obtained from the return value of the task's function

    SerializationError
        When some of the outputs cannot be serialized with the specified Serializer
    """
    if isinstance(node, DAG):
        return _invoke_dag(node, params=params)
    elif isinstance(node, Task):
        return _invoke_task(node, params=params)
    else:
        raise NotImplementedError(
            f"Whoops, we were not expecting a node of type '{type(node).__name__}'. This type of nodes are not supported by the local runtime at the moment. If you believe this may be a bug, please report it to our GitHub repository."
        )


def _invoke_dag(
    dag: DAG,
    params: Optional[Mapping[str, Any]] = None,
) -> NodeOutputs:
    params = params or {}

    outputs: Dict[str, NodeExecutions] = {}

    validate_parameters(dag.inputs, params)

    sequential_node_order = itertools.chain(*dag.node_execution_order)
    for node_name in sequential_node_order:
        node = dag.nodes[node_name]

        try:
            outputs[node_name] = [
                invoke(node, params=p)
                for p in _node_param_partitions(
                    node=node,
                    params=params,
                    outputs=outputs,
                )
            ]

            if not node.partition_by_input:
                outputs[node_name] = outputs[node_name][0]

        except (ValueError, TypeError, SerializationError) as e:
            raise e.__class__(f"Error when invoking node '{node_name}'. {str(e)}")

    dag_outputs = {}
    for output_name in dag.outputs:
        # TODO: Extract to a private function and control outputs that don't come from other nodes
        from_node_output = dag.outputs[output_name]
        if dag.nodes[from_node_output.node].partition_by_input:
            dag_outputs[output_name] = [
                partition[from_node_output.output]
                for partition in outputs[from_node_output.node]
            ]
        else:
            dag_outputs[output_name] = outputs[from_node_output.node][
                from_node_output.output
            ]

    return dag_outputs


def _node_param_partitions(
    node: Node,
    params: Mapping[str, Any],
    outputs: Mapping[str, NodeOutputs],
) -> Partitioned[NodeParams]:
    fixed_params = {
        name: _node_param(
            input_name=name,
            input_type=node.inputs[name],
            params=params,
            outputs=outputs,
        )
        for name in node.inputs.keys() - {node.partition_by_input}
    }

    if node.partition_by_input:
        input_value = _node_param(
            input_name=node.partition_by_input,
            input_type=node.inputs[node.partition_by_input],
            params=params,
            outputs=outputs,
        )
        if not isinstance(input_value, Partitioned):
            raise TypeError(
                f"This node is supposed to be partitioned by input '{node.partition_by_input}'. When a node is partitioned, the value of the input that determines the partition should be a list. Instead, we found a value of type '{type(input_value).__name__}'."
            )

        return [{node.partition_by_input: p, **fixed_params} for p in input_value]
    else:
        return [fixed_params]


def _node_param(
    input_name: str,
    input_type: Union[FromParam, FromNodeOutput],
    params: Mapping[str, Any],
    outputs: Mapping[str, NodeOutputs],
) -> Any:
    if isinstance(input_type, FromParam):
        return params[input_type.name or input_name]
    elif isinstance(input_type, FromNodeOutput):
        if isinstance(outputs[input_type.node], Partitioned):
            return [
                _node_param_from_output(
                    serializer=input_type.serializer,
                    node_output=partition[input_type.output],
                )
                for partition in outputs[input_type.node]
            ]
        else:
            return _node_param_from_output(
                serializer=input_type.serializer,
                node_output=outputs[input_type.node][input_type.output],
            )

    else:
        raise TypeError(
            f"Input type '{type(input_type)}' is not supported by the local runtime. The use of unsupported inputs should have been validated by the DAG object. This may be a bug in the library. Please open an issue in our GitHub repository."
        )


def _node_param_from_output(
    serializer: Serializer,
    node_output: NodeOutput,
) -> Union[Any, Partitioned[Any]]:
    if isinstance(node_output, Partitioned):
        return [serializer.deserialize(v) for v in node_output]
    else:
        return serializer.deserialize(node_output)
