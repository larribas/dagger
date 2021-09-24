"""Store node outputs in the local filesystem and load them back."""
from typing import Any, Mapping

from dagger.runtime.local.types import NodeOutputs, OutputFile, PartitionedOutput
from dagger.serializer import Serializer


def load(filename: str, serializer: Serializer) -> Any:
    """Load a value from the file system."""
    with open(filename, "rb") as reader:
        return serializer.deserialize(reader)


def dump(
    filename: str,
    value: Any,
    serializer: Serializer,
) -> OutputFile:
    """Dump a value into a file in the specified path and return the filename."""
    with open(filename, "wb") as writer:
        serializer.serialize(value, writer)

    return OutputFile(filename=filename, serializer=serializer)


def deserialized_outputs(node_outputs: NodeOutputs) -> Mapping[str, Any]:
    """Return the outputs of a node, given the node outputs returned when invoking it."""
    results = {}

    for name, node_output in node_outputs.items():
        if isinstance(node_output, PartitionedOutput):
            partition_values = []
            for partition in node_output:
                with open(partition.filename, "rb") as reader:
                    partition_values.append(partition.serializer.deserialize(reader))
            results[name] = partition_values

        else:
            with open(node_output.filename, "rb") as reader:
                results[name] = node_output.serializer.deserialize(reader)

    return results
