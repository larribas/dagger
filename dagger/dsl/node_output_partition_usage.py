"""Data structure that represents the usage of a partition from a node output."""


from typing import Iterator

from dagger.dsl.node_output_reference import NodeOutputReference
from dagger.serializer import Serializer


class NodeOutputPartitionUsage:
    """
    Represents the usage of a partition from a node output.

    An instance of this class is returned whenever an output reference is iterated over.

    ```
    @dsl.task
    def f() -> dict:
        return [1, 2, 3]

    @dsl.task
    def g(n: int):
        print(n)

    @dsl.DAG
    def dag():
        numbers = f()
        for n in numbers:
            g(n)
    ```

    In the previous example, `numbers` will be an instance of NodeOutputUsage, but `n` will be an instance of NodeOutputPartitionUsage, wrapping the NodeOutputUsage instance.
    """

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
        return self._wrapped_reference.is_partitioned

    @property
    def wrapped_reference(self) -> NodeOutputReference:
        """Return the node output reference wrapped by the partition."""
        return self._wrapped_reference

    def __iter__(self) -> Iterator:
        """Return an Iterator over the partitions of this output."""
        raise ValueError(
            "When defining DAGs through the DSL, you can iterate over the output of a node, if the output of that node is supposed to be partitioned. However, you may not iterate over one of the partitions of that output."
        )

    def __repr__(self) -> str:
        """Get a human-readable string representation of this instance."""
        return f"NodeOutputPartitionUsage(wrapped_reference={self._wrapped_reference})"

    def __eq__(self, obj) -> bool:
        """Return true if both objects are equivalent."""
        return (
            isinstance(obj, NodeOutputPartitionUsage)
            and self._wrapped_reference == obj._wrapped_reference
        )
