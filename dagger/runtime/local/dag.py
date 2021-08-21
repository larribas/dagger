"""Run a DAG in memory."""
import itertools
from typing import Any, Dict, List, Mapping, Optional, Union

from dagger.dag import DAG, Node, validate_parameters
from dagger.input import FromNodeOutput, FromParam
from dagger.runtime.local.task import _invoke_task
from dagger.runtime.local.types import NodeOutput, NodeParams, NodePartitions
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
) -> Mapping[str, NodeOutput]:
    params = params or {}

    outputs: Dict[str, NodePartitions] = {}

    validate_parameters(dag.inputs, params)

    sequential_node_order = itertools.chain(*dag.node_execution_order)
    for node_name in sequential_node_order:
        node = dag.nodes[node_name]

        # Depending on whether or not the node is partitioned,
        # we will need to execute it any number of times.
        node_param_partitions = _node_param_partitions(
            node=node,
            params=params,
            outputs=outputs,
        )

        # TODO: Try if the code would be simplified by making everything a partition, and assuming 1 is the default number of partitions
        try:
            if len(node_param_partitions) == 1:
                outputs[node_name] = invoke(node, params=node_param_partitions[0])
            else:
                outputs[node_name] = [
                    invoke(node, params=p) for p in node_param_partitions
                ]

        except (ValueError, TypeError, SerializationError) as e:
            raise e.__class__(f"Error when invoking node '{node_name}'. {str(e)}")

    dag_outputs = {}
    for output_name in dag.outputs:
        from_node_output = dag.outputs[output_name]
        output_value = outputs[from_node_output.node]
        if isinstance(output_value, list):
            dag_outputs[output_name] = itertools.chain(*output_value)
        else:
            dag_outputs[output_name] = output_value[from_node_output.output]

    return dag_outputs


def _node_param_partitions(
    node: Node,
    params: Mapping[str, Any],
    outputs: Mapping[str, NodePartitions],
) -> List[NodeParams]:

    fixed_param_names = node.inputs.keys() - {node.partition_by_input}

    fixed_params = {
        name: _node_param(
            input_name=name,
            input_type=node.inputs[name],
            params=params,
            outputs=outputs,
        )
        for name in fixed_param_names
    }

    if node.partition_by_input:
        partitioned_output = _node_param(
            input_name=node.partition_by_input,
            input_type=node.inputs[node.partition_by_input],
            params=params,
            outputs=outputs,
        )
        # TODO: If we don't end up treating everything as a list, we'll have to control the error of partitioned_output not being a list here.
        return [
            {node.partition_by_input: p, **fixed_params} for p in partitioned_output
        ]
    else:
        return [fixed_params]


def _node_param(
    input_name: str,
    input_type: Union[FromParam, FromNodeOutput],
    params: Mapping[str, Any],
    outputs: Mapping[str, NodePartitions],
) -> Any:
    if isinstance(input_type, FromParam):
        return params[input_type.name or input_name]
    elif isinstance(input_type, FromNodeOutput):
        # TODO: If the output is a list, deserialize each individual item
        return _deserialize_from_node_output_partitions(
            input_type=input_type,
            partitions=outputs[input_type.node],
        )
    else:
        raise TypeError(
            f"Input type '{type(input_type)}' is not supported by the local runtime. The use of unsupported inputs should have been validated by the DAG object. This may be a bug in the library. Please open an issue in our GitHub repository."
        )


def _deserialize_from_node_output_partitions(
    input_type: FromNodeOutput,
    partitions: NodePartitions,
) -> Any:
    if isinstance(partitions, list):
        return [
            _deserialize_output(
                serializer=input_type.serializer,
                output_value=v[input_type.output],
            )
            for v in partitions
        ]
    else:
        return _deserialize_output(
            serializer=input_type.serializer,
            output_value=partitions[input_type.output],
        )


def _deserialize_output(
    serializer: Serializer,
    output_value: NodeOutput,
) -> Any:
    if isinstance(output_value, list):
        # TODO: Either control error or assume everything is a list
        return [serializer.deserialize(v) for v in output_value]
    else:
        return serializer.deserialize(output_value)
