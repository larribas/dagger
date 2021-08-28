"""Run tasks in memory."""
import warnings
from typing import Any, Dict, Iterable, List, Mapping, Optional

from dagger.runtime.local.types import NodeOutput, NodeOutputs, PartitionedOutput
from dagger.serializer import SerializationError
from dagger.task import SupportedInputs, SupportedOutputs, Task


def _invoke_task(
    task: Task,
    params: Optional[Mapping[str, Any]] = None,
) -> NodeOutputs:
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
) -> Mapping[str, NodeOutput]:

    node_outputs: Dict[str, List[bytes]] = {}
    for output_name in outputs:
        output_type = outputs[output_name]
        try:
            node_outputs[output_name] = _serialize_output(
                output_name=output_name,
                output_type=outputs[output_name],
                output_value=output_type.from_function_return_value(return_value),
            )

        except (TypeError, ValueError, SerializationError) as e:
            raise e.__class__(
                f"We encountered the following error while attempting to serialize the results of this task: {str(e)}"
            ) from e

    return node_outputs


def _serialize_output(
    output_name: str,
    output_type: SupportedOutputs,
    output_value: Any,
) -> NodeOutput:
    if output_type.is_partitioned:
        if not isinstance(output_value, Iterable):
            raise TypeError(
                f"Output '{output_name}' was declared as a partitioned output, but the return value was not an iterable (instead, it was of type '{type(output_value).__name__}'). Partitioned outputs should be iterables of values (e.g. lists or sets). Each value in the iterable must be serializable with the serializer defined in the output."
            )

        return PartitionedOutput(
            map(lambda o: output_type.serializer.serialize(o), output_value)
        )
    else:
        return output_type.serializer.serialize(output_value)
