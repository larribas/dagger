"""Protocol that represents a reference to a node output."""

from typing import Iterator, Protocol, runtime_checkable

from dagger.serializer import Serializer


@runtime_checkable
class NodeOutputReference(Protocol):  # pragma: no cover
    """
    Protocol that references a specific output of a node.

    It will be received as an argument by a task. For instance, when doing:

    ```
    @dsl.task
    def f() -> int:
        return 2

    @dsl.task
    def g(number: int):
        print(number)

    @dsl.DAG
    def dag():
        f_output = f()
        g(f_output)
    ```


    The function `g` will receive a node output reference it can use to build its inputs.
    """

    @property
    def invocation_id(self) -> str:
        """Return the invocation id of this reference."""
        ...

    @property
    def output_name(self) -> str:
        """Return the output name of this reference."""
        ...

    @property
    def serializer(self) -> Serializer:
        """Return the serializer assigned to this output."""
        ...

    @property
    def is_partitioned(self) -> bool:
        """Return true if the output is partitioned. This happens whenever the output reference is iterated upon."""
        ...

    @property
    def references_node_partition(self) -> bool:
        """Return true if the output comes from a partitioned node.."""
        ...

    def __iter__(self) -> Iterator:
        """Return an Iterator over the partitions of this output."""
        ...

    def consume(self):
        """Mark this output as consumed by another node."""
        ...
