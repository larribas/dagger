"""Data structure that represents the usage of a node output."""

from typing import Iterator, Set

from dagger.dsl.node_output_key_usage import NodeOutputKeyUsage
from dagger.dsl.node_output_partition_usage import NodeOutputPartitionUsage
from dagger.dsl.node_output_property_usage import NodeOutputPropertyUsage
from dagger.dsl.node_output_reference import NodeOutputReference
from dagger.dsl.node_output_serializer import NodeOutputSerializer
from dagger.serializer import Serializer


class NodeOutputUsage:
    """
    Represents the usage of a node output.

    An instance of this class is returned whenever a task is invoked. For example:

    ```
    @dsl.task
    def f() -> dict:
        return {"a": 1, "b": 2}

    @dsl.DAG
    def dag():
        f_output = f()
    ```

    In the previous example, `f_output` will be an instance of this class.

    Instances can be passed as inputs to other tasks:

    ```
    @dsl.DAG
    def dag():
        f_output = f()
        g(f_output)
    ```

    If the output is a mapping, you can access a specific key:

    ```
    @dsl.DAG
    def dag():
        f_output = f()
        g(f_output["a"])
    ```

    If the output is an object, you can access a specific property:

    ```
    @dsl.DAG
    def dag():
        f_output = f()
        g(f_output.a)
    ```
    """

    def __init__(
        self,
        invocation_id: str,
        serializer: NodeOutputSerializer = NodeOutputSerializer(),
        references_node_partition: bool = False,
    ):
        self._invocation_id = invocation_id
        self._references: Set[NodeOutputReference] = set()
        self._serializer = serializer
        self._references_node_partition = references_node_partition
        self._is_partitioned = False

    @property
    def invocation_id(self) -> str:
        """Return the invocation id of the task that generated this output."""
        return self._invocation_id

    @property
    def output_name(self) -> str:
        """
        Return the name of this output.

        The output name of the return value is always "return_value".
        All other outputs (keys and properties) are namespaced to guarantee
        that there will never be two outputs of the same node with the same name.
        """
        return "return_value"

    @property
    def serializer(self) -> Serializer:
        """Return the serializer assigned to this output."""
        return self._serializer.root

    @property
    def is_partitioned(self) -> bool:
        """Return true if the output is partitioned. This happens whenever the output reference is iterated upon."""
        return self._is_partitioned

    @property
    def references_node_partition(self) -> bool:
        """Return true if the output comes from a partitioned node.."""
        return self._references_node_partition

    @property
    def references(self) -> Set[NodeOutputReference]:
        """
        Return all the ways this output has been referenced.

        If the output has never been used as input for another node, it will be empty.
        """
        return self._references

    def consume(self):
        """
        Mark this output as consumed by another node.

        When a node returns an output of type `NodeOutputUsage` and that output
        is passed to another node, there is ambiguity about whether or not the
        output was used or not.

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

        Invoking this method removes that ambiguity by explicitly telling the object
        that another node depends on it, and thus it needs to be included as part of
        the outputs of the node that produced it.

        If this function is not explicitly invoked, we will assume the return value
        of this node is not used directly, and therefore the `FromReturnValue` output
        will not be generated for it.
        """
        self._references.add(self)

    def __getattr__(self, name) -> NodeOutputReference:
        """Access an attribute of the output. The output must be an object."""
        if name.startswith("__") and name.endswith("__"):
            raise ValueError(
                f"You are trying to reference attribute named '{name}'. Attributes of the form __name__ are used internally by Python and they are not available within the scope of the dagger DSL."
            )

        ref = NodeOutputPropertyUsage(
            invocation_id=self._invocation_id,
            output_name=f"property_{name}",
            property_name=name,
            serializer=self._serializer.sub_output(name) or self._serializer.root,
            references_node_partition=self._references_node_partition,
        )
        self._references.add(ref)
        return ref

    def __getitem__(self, name) -> NodeOutputReference:
        """Access a key of the output. The output must be a mapping."""
        ref = NodeOutputKeyUsage(
            invocation_id=self._invocation_id,
            output_name=f"key_{name}",
            key_name=name,
            serializer=self._serializer.sub_output(name) or self._serializer.root,
            references_node_partition=self._references_node_partition,
        )
        self._references.add(ref)
        return ref

    def __iter__(self) -> Iterator:
        """Return an Iterator over the partitions of this output."""
        self._is_partitioned = True
        self._references.add(self)
        return iter([NodeOutputPartitionUsage(self)])

    def __repr__(self) -> str:
        """Get a human-readable string representation of this output usage."""
        return f"NodeOutputUsage(invocation_id={self._invocation_id})"

    def __eq__(self, obj) -> bool:
        """Return true if both objects are equivalent."""
        return (
            isinstance(obj, NodeOutputUsage)
            and self._invocation_id == obj._invocation_id
            and self._is_partitioned == obj._is_partitioned
            and self._references_node_partition == obj._references_node_partition
        )

    def __hash__(self) -> int:
        """
        Return a hash of the current object.

        This is used to determine whether two references are equivalent and should only be listed once in the set.
        """
        return hash((self._invocation_id))
