"""Invoke a node and store/returns its outputs."""
import tempfile
from typing import Any, Mapping, NamedTuple, Union

from dagger.dag import Node
from dagger.runtime.local.dag import invoke_node
from dagger.runtime.local.output import deserialized_outputs


class ReturnDeserializedOutputs:
    """Indicates that the outputs of a node invoked with the local runtime should be returned in their deserialized format."""

    pass


class StoreSerializedOutputsInPath(NamedTuple):
    """Indicates that the outputs of a node invoked with the local runtime should be stored in files in the local filesystem and the invocation should return pointers to those files."""

    path: str


def invoke(
    node: Node,
    params: Mapping[str, Any] = None,
    outputs: Union[
        ReturnDeserializedOutputs, StoreSerializedOutputsInPath
    ] = ReturnDeserializedOutputs(),
) -> Mapping[str, Any]:
    """
    Invoke a node with a series of parameters.

    Parameters
    ----------
    node
        Node to execute

    params
        Inputs to the task, indexed by input/parameter name.

    outputs
        An indication of what to do with the node's outputs.
        When set to ReturnDeserializedOutputs, it returns a mapping of
        output name to output value (deserialized).
        When set to StoreSerializedOutputsInPath, it returns a mapping of
        output name to filepath, where filepath contains the serialized value
        of that output.

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
    params = params or {}

    if isinstance(outputs, StoreSerializedOutputsInPath):
        return invoke_node(node=node, output_path=outputs.path, params=params)

    with tempfile.TemporaryDirectory() as tmp:
        node_outputs = invoke_node(node=node, output_path=tmp, params=params)
        return deserialized_outputs(node_outputs)
