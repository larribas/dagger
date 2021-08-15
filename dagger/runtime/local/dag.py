"""Run a DAG in memory."""
import itertools
from typing import Any, Dict, Mapping, Optional

from dagger.dag import DAG, validate_parameters
from dagger.input import FromNodeOutput, FromParam
from dagger.runtime.local.task import _invoke_task
from dagger.serializer import SerializationError


def _invoke_dag(
    dag: DAG,
    params: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, bytes]:
    params = params or {}
    outputs: Dict[str, Mapping[str, bytes]] = {}

    validate_parameters(dag.inputs, params)

    # TODO: Support both sequential and parallel execution
    sequential_node_order = itertools.chain(*dag.node_execution_order)
    for node_name in sequential_node_order:
        node = dag.nodes[node_name]
        node_params: Dict[str, bytes] = {}
        for input_name in node.inputs:
            node_input = node.inputs[input_name]
            if isinstance(node_input, FromParam):
                node_params[input_name] = params[node_input.name or input_name]
            elif isinstance(node_input, FromNodeOutput):
                node_params[input_name] = node_input.serializer.deserialize(
                    outputs[node_input.node][node_input.output]
                )
            else:
                raise TypeError(
                    f"Input type '{type(node_input)}' is not supported by the local runtime. The use of unsupported inputs should have been validated by the DAG object. This may be a bug in the library. Please open an issue in our GitHub repository."
                )

        try:
            if isinstance(node, DAG):
                outputs[node_name] = _invoke_dag(node, params=node_params)
            else:
                outputs[node_name] = _invoke_task(node, params=node_params)
        except (ValueError, TypeError, SerializationError) as e:
            raise e.__class__(f"Error when invoking node '{node_name}'. {str(e)}")

    dag_outputs = {}
    for output_name in dag.outputs:
        output_type = dag.outputs[output_name]
        dag_outputs[output_name] = outputs[output_type.node][output_type.output]

    return dag_outputs
