from typing import Any, Dict, Optional

from argo_workflows_sdk.node import Node, SupportedInputs, SupportedOutputs
from argo_workflows_sdk.outputs import Output
from argo_workflows_sdk.serializers import SerializationError


def invoke(
    node: Node,
    params: Optional[Dict[str, bytes]] = None,
) -> Dict[str, bytes]:

    params = params or {}

    inputs = deserialize_inputs(
        inputs=node.inputs,
        params=params,
    )

    return_value = node.func(**inputs)

    return serialize_outputs(
        outputs=node.outputs,
        return_value=return_value,
    )


def deserialize_inputs(
    inputs: Dict[str, SupportedInputs],
    params: Dict[str, bytes],
):

    deserialized_inputs = {}
    for input_name, input_type in inputs.items():
        try:
            deserialized_inputs[input_name] = input_type.serializer.deserialize(
                params[input_name]
            )
        except KeyError:
            raise ValueError(
                f"The parameters supplied to this node were supposed to contain a parameter named '{input_name}', but only the following parameters were actually supplied: {list(params.keys())}"
            )

    return deserialized_inputs


def serialize_outputs(
    outputs: Dict[str, SupportedOutputs],
    return_value: Any,
) -> Dict[str, bytes]:

    serialized_outputs = {}
    for output_name, output_type in outputs.items():
        try:
            output = output_type.from_function_return_value(return_value)
            serialized_outputs[output_name] = output_type.serializer.serialize(output)
        except (TypeError, ValueError, SerializationError) as e:
            raise e.__class__(
                f"We encountered the following error while attempting to serialize the results of this node: {str(e)}"
            )

    return serialized_outputs
