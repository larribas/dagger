"""Run a DAG in memory."""
import itertools
from typing import Dict, Optional

from dagger.dag import DAG, validate_parameters
from dagger.input import FromNodeOutput, FromParam
from dagger.runtime.local.task import invoke_task
from dagger.serializer import SerializationError


def invoke_dag(
    dag: DAG,
    params: Optional[Dict[str, bytes]] = None,
) -> Dict[str, bytes]:
    """
    Invoke a DAG with a series of parameters.

    Parameters
    ----------
    dag
        DAG to execute

    params
        Inputs to the DAG.
        Serialized into their binary format.
        Indexed by input/parameter name.


    Returns
    -------
    Serialized outputs of the DAG, indexed by output name.


    Raises
    ------
    ValueError
        When any required parameters are missing

    TypeError
        When any of the outputs cannot be obtained from the return value of their task

    SerializationError
        When some of the outputs cannot be serialized with the specified Serializer
    """
    params = params or {}
    outputs: Dict[str, Dict[str, bytes]] = {}

    validate_parameters(dag.inputs, params)

    # TODO: Support both sequential and parallel execution
    sequential_task_order = itertools.chain(*dag.node_execution_order)
    for task_name in sequential_task_order:
        task = dag.nodes[task_name]
        task_params: Dict[str, bytes] = {}
        for input_name, input_type in task.inputs.items():
            if isinstance(input_type, FromParam):
                task_params[input_name] = params[input_name]
            elif isinstance(input_type, FromNodeOutput):
                task_params[input_name] = outputs[input_type.node][input_type.output]
            else:
                raise TypeError(
                    f"Input type '{type(input_type)}' is not supported by the local runtime. The use of unsupported inputs should have been validated by the DAG object. This may be a bug in the library. Please open an issue in our GitHub repository."
                )

        try:
            if isinstance(task, DAG):
                outputs[task_name] = invoke_dag(task, params=task_params)
            else:
                outputs[task_name] = invoke_task(task, params=task_params)
        except (ValueError, TypeError, SerializationError) as e:
            raise e.__class__(f"Error when invoking task '{task_name}'. {str(e)}")

    dag_outputs = {}
    for output_name, output in dag.outputs.items():
        dag_outputs[output_name] = outputs[output.node][output.output]

    return dag_outputs
