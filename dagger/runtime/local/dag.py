"""Run a DAG in memory."""
import itertools
from typing import Dict, Optional

from dagger.dag import DAG, validate_parameters
from dagger.inputs import FromNodeOutput, FromParam
from dagger.runtime.local.node import invoke_node
from dagger.serializers import SerializationError


def invoke_dag(
    dag: DAG,
    params: Optional[Dict[str, bytes]] = None,
) -> Dict[str, bytes]:
    """
    Invoke a DAG with a series of parameters.

    Parameters
    ----------
    dag : DAG
        DAG to execute

    params : Dictionary of str -> bytes
        Inputs to the DAG.
        Serialized into their binary format.
        Indexed by input/parameter name.


    Returns
    -------
    Dictionary of str -> bytes
        Serialized outputs of the DAG.
        Indexed by output name.


    Raises
    ------
    ValueError
        When any required parameters are missing

    TypeError
        When any of the outputs cannot be obtained from the return value of their node

    SerializationError
        When some of the outputs cannot be serialized with the specified Serializer
    """
    params = params or {}
    outputs: Dict[str, Dict[str, bytes]] = {}

    validate_parameters(dag.inputs, params)

    # TODO: Support both sequential and parallel execution
    sequential_node_order = itertools.chain(*dag.node_execution_order)
    for node_name in sequential_node_order:
        node = dag.nodes[node_name]
        node_params: Dict[str, bytes] = {}
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
            if isinstance(node, DAG):
                outputs[node_name] = invoke_dag(node, params=node_params)
            else:
                outputs[node_name] = invoke_node(node, params=node_params)
        except (ValueError, TypeError, SerializationError) as e:
            raise e.__class__(f"Error when invoking node '{node_name}'. {str(e)}")

    dag_outputs = {}
    for output_name, output in dag.outputs.items():
        dag_outputs[output_name] = outputs[output.node][output.output]

    return dag_outputs
