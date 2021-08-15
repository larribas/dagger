"""Run tasks in memory."""
import warnings
from typing import Any, Mapping, Optional

from dagger.serializer import SerializationError
from dagger.task import SupportedInputs, SupportedOutputs, Task


def invoke_task(
    task: Task,
    params: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, bytes]:
    """
    Invoke a task with a series of parameters.

    Parameters
    ----------
    task
        Task to execute

    params
        Inputs to the task, indexed by input/parameter name.


    Returns
    -------
    Mappingionary of str -> bytes
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
    params = params or {}
    inputs = _validate_and_filter_inputs(inputs=task.inputs, params=params)

    return_value = task.func(**inputs)

    return _serialize_outputs(
        outputs=task.outputs,
        return_value=return_value,
    )


def _validate_and_filter_inputs(
    inputs: Mapping[str, SupportedInputs],
    params: Mapping[str, Any],
) -> Mapping[str, Any]:
    missing_params = inputs.keys() - params.keys()
    if missing_params:
        raise ValueError(
            f"The following parameters are required by the task but were not supplied: {sorted(list(missing_params))}"
        )

    superfluous_params = params.keys() - inputs.keys()
    if superfluous_params:
        warnings.warn(
            f"The following parameters were supplied to the task, but are not necessary: {sorted(list(superfluous_params))}"
        )

    return {input_name: params[input_name] for input_name in inputs}


def _serialize_outputs(
    outputs: Mapping[str, SupportedOutputs],
    return_value: Any,
) -> Mapping[str, bytes]:

    serialized_outputs = {}
    for output_name in outputs:
        output_type = outputs[output_name]
        try:
            output = output_type.from_function_return_value(return_value)
            serialized_outputs[output_name] = output_type.serializer.serialize(output)
        except (TypeError, ValueError, SerializationError) as e:
            raise e.__class__(
                f"We encountered the following error while attempting to serialize the results of this task: {str(e)}"
            )

    return serialized_outputs
