"""Run tasks in memory."""
from typing import Any, Mapping, Optional

from dagger.serializer import SerializationError
from dagger.task import SupportedInputs, SupportedOutputs, Task


def invoke_task(
    task: Task,
    params: Optional[Mapping[str, bytes]] = None,
) -> Mapping[str, bytes]:
    """
    Invoke a task with a series of parameters.

    Parameters
    ----------
    task
        Task to execute

    params
        Inputs to the task.
        Serialized into their binary format.
        Indexed by input/parameter name.


    Returns
    -------
    Mappingionary of str -> bytes
        Serialized outputs of the task.
        Indexed by output name.


    Raises
    ------
    ValueError
        When any required parameters are missing

    TypeError
        When any of the outputs cannot be obtained from the return value of the task's function

    SerializationError
        When some of the outputs cannot be serialized with the specified Serializer
    """
    params = params or {}

    inputs = _deserialize_inputs(
        inputs=task.inputs,
        params=params,
    )

    return_value = task.func(**inputs)

    return _serialize_outputs(
        outputs=task.outputs,
        return_value=return_value,
    )


def _deserialize_inputs(
    inputs: Mapping[str, SupportedInputs],
    params: Mapping[str, bytes],
):

    deserialized_inputs = {}
    for input_name, input_type in inputs.items():
        try:
            deserialized_inputs[input_name] = input_type.serializer.deserialize(
                params[input_name]
            )
        except KeyError:
            raise ValueError(
                f"The parameters supplied to this task were supposed to contain a parameter named '{input_name}', but only the following parameters were actually supplied: {list(params.keys())}"
            )

    return deserialized_inputs


def _serialize_outputs(
    outputs: Mapping[str, SupportedOutputs],
    return_value: Any,
) -> Mapping[str, bytes]:

    serialized_outputs = {}
    for output_name, output_type in outputs.items():
        try:
            output = output_type.from_function_return_value(return_value)
            serialized_outputs[output_name] = output_type.serializer.serialize(output)
        except (TypeError, ValueError, SerializationError) as e:
            raise e.__class__(
                f"We encountered the following error while attempting to serialize the results of this task: {str(e)}"
            )

    return serialized_outputs
