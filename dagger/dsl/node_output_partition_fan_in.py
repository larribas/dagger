"""Data structure that represents the usage of a partition from a node output."""


from typing import Iterator

from dagger.dsl.node_output_reference import NodeOutputReference
from dagger.serializer import Serializer


class NodeOutputPartitionFanIn:
    """Represents the usage of multiple partitions in a reduce/fan-in operation."""

    def __init__(
        self,
        wrapped_reference: NodeOutputReference,
    ):
        self._wrapped_reference = wrapped_reference

    @property
    def invocation_id(self) -> str:
        """Return the invocation id of this reference."""
        return self._wrapped_reference.invocation_id

    @property
    def output_name(self) -> str:
        """Return the output name of this reference."""
        return self._wrapped_reference.output_name

    @property
    def serializer(self) -> Serializer:
        """Return the serializer assigned to this output."""
        return self._wrapped_reference.serializer

    @property
    def is_partitioned(self) -> bool:
        """Return true if the output is partitioned. This happens whenever the output reference is iterated upon."""
        return False

    @property
    def references_node_partition(self) -> bool:
        """Return true if the output comes from a partitioned node.."""
        return False

    def consume(self):
        """Mark this output as consumed by another node."""
        self._wrapped_reference.consume()

    @property
    def wrapped_reference(self) -> NodeOutputReference:
        """Return the node output reference wrapped by the partition."""
        return self._wrapped_reference

    def __iter__(self) -> Iterator:
        """Return an Iterator over the partitions of this output."""
        raise NotImplementedError

    def __repr__(self) -> str:
        """Get a human-readable string representation of this instance."""
        return f"NodeOutputPartitionFanIn(wrapped_reference={self._wrapped_reference})"

    def __eq__(self, obj) -> bool:
        """Return true if both objects are equivalent."""
        return (
            isinstance(obj, NodeOutputPartitionFanIn)
            and self._wrapped_reference == obj._wrapped_reference
        )
