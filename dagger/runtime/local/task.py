"""Run tasks in memory."""
import os
from typing import Any, Dict, Iterable, Mapping

from dagger.input import validate_parameters
from dagger.runtime.local.output import dump
from dagger.runtime.local.types import NodeOutput, NodeOutputs, PartitionedOutput
from dagger.serializer import SerializationError
from dagger.task import SupportedInputs, SupportedOutputs, Task


def invoke_task(
    task: Task,
    params: Mapping[str, Any],
    output_path: str,
) -> NodeOutputs:
    """Invoke a task locally with the specified parameters and dump the serialized outputs on the path provided."""
    validate_parameters(task.inputs, params)
    inputs = _filter_inputs(inputs=task.inputs, params=params)

    return_value = task.func(**inputs)

    return _serialize_outputs(
        path=output_path,
        outputs=task.outputs,
        return_value=return_value,
    )


def _filter_inputs(
    inputs: Mapping[str, SupportedInputs],
    params: Mapping[str, Any],
) -> Mapping[str, Any]:

    return {
        input_name: params[input_name]
        if input_name in params
        else input_value.default_value
        for input_name, input_value in inputs.items()
    }


def _serialize_outputs(
    path: str,
    outputs: Mapping[str, SupportedOutputs],
    return_value: Any,
) -> Mapping[str, NodeOutput]:

    node_outputs: Dict[str, NodeOutput] = {}
    for output_name in outputs:
        output_type = outputs[output_name]
        try:
            node_outputs[output_name] = _serialize_output(
                path=path,
                name=output_name,
                value=output_type.from_function_return_value(return_value),
                type_=outputs[output_name],
            )

        except (TypeError, ValueError, SerializationError) as e:
            raise e.__class__(
                f"We encountered the following error while attempting to serialize the results of this task: {str(e)}"
            ) from e

    return node_outputs


def _serialize_output(
    path: str,
    name: str,
    value: Any,
    type_: SupportedOutputs,
) -> NodeOutput:
    if type_.is_partitioned:
        if not isinstance(value, Iterable):
            raise TypeError(
                f"Output '{name}' was declared as a partitioned output, but the return value was not an iterable (instead, it was of type '{type(value).__name__}'). Partitioned outputs should be iterables of values (e.g. lists or sets). Each value in the iterable must be serializable with the serializer defined in the output."
            )

        return PartitionedOutput(
            map(
                lambda v: dump(
                    filename=os.path.join(path, name),
                    serializer=type_.serializer,
                    value=v,
                ),
                value,
            )
        )
    else:
        return dump(
            filename=os.path.join(path, name),
            value=value,
            serializer=type_.serializer,
        )
