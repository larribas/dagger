import itertools
from typing import Any, Dict, List, Optional

from argo_workflows_sdk.dag import DAG, SupportedInputs
from argo_workflows_sdk.inputs import FromNodeOutput, FromParam
from argo_workflows_sdk.runtime.local.node import invoke as invoke_node
from argo_workflows_sdk.serializers import SerializationError


def invoke(
    dag: DAG,
    # TODO: Allow passing plain unserialized parameters, here and in the node.invoke function
    params: Optional[Dict[str, bytes]] = None,
) -> Dict[str, bytes]:

    params = params or {}
    outputs = {}

    validate_parameters_match_dag_inputs(dag.inputs, params)

    # TODO: Support both sequential and parallel execution
    sequential_node_order = itertools.chain(*dag.node_execution_order)
    for node_name in sequential_node_order:
        node = dag.nodes[node_name]
        node_params = {}
        for input_name, input_type in node.inputs.items():
            if isinstance(input_type, FromParam):
                node_params[input_name] = params[input_name]
            elif isinstance(input_type, FromNodeOutput):
                node_params[input_name] = outputs[input_type.node][input_type.output]
            else:
                raise TypeError(
                    f"Input type '{type(input_type)}' is not supported by the local runtime. The use of unsupported inputs should have been validated by the DAG object. This may be a bug in the library. Please open an issue in our GitHub repository."
                )

        try:
            outputs[node_name] = invoke_node(node, params=node_params)
        except (ValueError, TypeError, SerializationError) as e:
            raise e.__class__(f"Error when invoking node '{node_name}'. {str(e)}")

    dag_outputs = {}
    for output_name, output in dag.outputs.items():
        dag_outputs[output_name] = outputs[output.node][output.output]

    return dag_outputs


def validate_parameters_match_dag_inputs(
    inputs: Dict[str, SupportedInputs],
    params: Dict[str, bytes],
):
    for input_name in inputs.keys():
        if input_name not in params:
            raise ValueError(
                f"The parameters supplied to this DAG were supposed to contain a parameter named '{input_name}', but only the following parameters were actually supplied: {list(params.keys())}"
            )
