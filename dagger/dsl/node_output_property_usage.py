"""Data structure that represents the usage of a specific property of a node output."""

from typing import Iterator

from dagger.dsl.node_output_partition_usage import NodeOutputPartitionUsage
from dagger.serializer import Serializer


class NodeOutputPropertyUsage:
    """
    Represents the usage of a specific property or attribute from a node output.

    The existence of this object implies that the output of a node
    has been used as such:

    ```
    @dsl.DAG
    def dag():
        f_output = f()
        g(f_output.a)
    ```

    And therefore, the node `f` needs to contain an output of type `output.FromProperty`
    """

    def __init__(
        self,
        invocation_id: str,
        output_name: str,
        property_name: str,
        serializer: Serializer,
        references_node_partition: bool = False,
    ):
        self._invocation_id = invocation_id
        self._output_name = output_name
        self._property_name = property_name
        self._serializer = serializer
        self._references_node_partition = references_node_partition
        self._is_partitioned = False

    @property
    def invocation_id(self) -> str:
        """Return the invocation id of this reference."""
        return self._invocation_id

    @property
    def output_name(self) -> str:
        """Return the output name of this reference."""
        return self._output_name

    @property
    def property_name(self) -> str:
        """Return the name of the property this reference points to."""
        return self._property_name

    @property
    def serializer(self) -> Serializer:
        """Return the serializer assigned to this output."""
        return self._serializer

    @property
    def is_partitioned(self) -> bool:
        """Return true if the output is partitioned. This happens whenever the output reference is iterated upon."""
        return self._is_partitioned

    @property
    def references_node_partition(self) -> bool:
        """Return true if the output comes from a partitioned node.."""
        return self._references_node_partition

    def consume(self):
        """Mark this output as consumed by another node."""
        pass

    def __iter__(self) -> Iterator:
        """Return an Iterator over the partitions of this output."""
        self._is_partitioned = True
        return iter([NodeOutputPartitionUsage(self)])

    def __repr__(self) -> str:
        """Get a human-readable string representation of this instance."""
        return f"NodeOutputPropertyUsage(invocation_id={self._invocation_id}, output_name={self._output_name}, property_name={self._property_name}, serializer={self._serializer}, is_partitioned={self._is_partitioned}, references_node_partition={self._references_node_partition})"

    def __eq__(self, obj) -> bool:
        """Return true if both objects are equivalent."""
        return (
            isinstance(obj, NodeOutputPropertyUsage)
            and self._invocation_id == obj._invocation_id
            and self._output_name == obj._output_name
            and self._property_name == obj._property_name
            and self._serializer == obj._serializer
            and self._is_partitioned == obj._is_partitioned
            and self._references_node_partition == obj._references_node_partition
        )

    def __hash__(self) -> int:
        """
        Return a hash that will be the same for two equivalent instances of this type.

        It's important to note that two tuples of this type will only be equal if all their attributes are exactly the same, but they will be equivalent if they reference the same invocation (by id) and property accessed (by name).
        """
        return hash((self.invocation_id, self.property_name))
