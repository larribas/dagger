"""Run a DAG in memory."""
import itertools
import os
from typing import Any, Dict, Iterable, Mapping, Union

from dagger.dag import DAG, Node, validate_parameters
from dagger.input import FromNodeOutput, FromParam
from dagger.runtime.local.output import load
from dagger.runtime.local.task import invoke_task
from dagger.runtime.local.types import (
    NodeExecutions,
    NodeOutput,
    NodeOutputs,
    NodeParams,
    PartitionedOutput,
)
from dagger.serializer import SerializationError, Serializer
from dagger.task import Task


def invoke_node(
    node: Union[DAG, Task],
    params: Mapping[str, Any],
    output_path: str,
) -> Mapping[str, NodeOutput]:
    """Invoke a Node locally with the specified parameters and dump the serialized outputs on the path provided."""
    if isinstance(node, DAG):
        return invoke_dag(node, output_path=output_path, params=params)
    else:
        return invoke_task(node, output_path=output_path, params=params)


def invoke_dag(
    dag: DAG,
    params: Mapping[str, Any],
    output_path: str,
) -> NodeOutputs:
    """Invoke a DAG locally with the specified parameters and dump the serialized outputs on the path provided."""
    validate_parameters(dag.inputs, params)

    outputs: Dict[str, NodeExecutions] = {}

    sequential_node_order = itertools.chain(*dag.node_execution_order)
    for node_name in sequential_node_order:
        node = dag.nodes[node_name]

        try:
            partitions = []

            for i, p in enumerate(
                _node_param_partitions(
                    node=node,
                    params=params,
                    outputs=outputs,
                )
            ):
                node_output_path = os.path.join(output_path, "nodes", node_name, str(i))
                os.makedirs(node_output_path)

                partitions.append(
                    invoke_node(node, params=p, output_path=node_output_path)
                )

            outputs[node_name] = PartitionedOutput(partitions)

            if not node.partition_by_input:
                outputs[node_name] = next(outputs[node_name])

        except (ValueError, TypeError, SerializationError) as e:
            raise e.__class__(
                f"Error when invoking node '{node_name}'. {str(e)}"
            ) from e

    dag_outputs = {
        output_name: outputs[output_type.node][output_type.output]
        for output_name, output_type in dag.outputs.items()
    }

    return dag_outputs


def _node_param_partitions(
    node: Node,
    params: Mapping[str, Any],
    outputs: Mapping[str, NodeOutputs],
) -> Iterable[NodeParams]:
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
        if not isinstance(input_value, Iterable):
            raise TypeError(
                f"This node is supposed to be partitioned by input '{node.partition_by_input}'. When a node is partitioned, the value of the input that determines the partition should be an iterable. Instead, we found a value of type '{type(input_value).__name__}'."
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
    elif isinstance(outputs[input_type.node], PartitionedOutput):
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


def _node_param_from_output(
    serializer: Serializer,
    node_output: NodeOutput,
) -> Union[Any, PartitionedOutput[Any]]:
    if isinstance(node_output, PartitionedOutput):
        return PartitionedOutput(
            map(lambda n: load(filename=n.filename, serializer=serializer), node_output)
        )
    else:
        return load(filename=node_output.filename, serializer=serializer)
