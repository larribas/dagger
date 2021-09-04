"""Data types used for local invocations."""

from typing import Any, Generic, Iterable, Iterator, Mapping, TypeVar, Union

T = TypeVar("T")


class PartitionedOutput(Generic[T]):
    """Represents a partitioned output explicitly."""

    def __init__(self, iterable: Iterable[T]):
        """Build a partitioned output from an Iterable."""
        self._iterable = iterable
        self._iterator = iter(iterable)

    def __iter__(self) -> Iterator[T]:
        """Return an iterator over the partitions of the output."""
        return self

    def __next__(self) -> T:
        """Return the next element in the partitioned output."""
        return next(self._iterator)

    def __repr__(self) -> str:
        """Return a human-readable representation of the partitioned output."""
        return repr(self._iterable)


#: One of the outputs of a node, which may be partitioned
NodeOutput = Union[bytes, PartitionedOutput[bytes]]

#: All outputs of a node indexed by their name. Node executions may be partitioned, in which case this is a list.
NodeOutputs = Mapping[str, NodeOutput]

#: All executions of a node. If the node is partitioned there will only be one. Otherwise, there may be many.
NodeExecutions = Union[NodeOutputs, PartitionedOutput[NodeOutputs]]

#: The parameters supplied to a node (plain, not serialized)
NodeParams = Mapping[str, Any]
