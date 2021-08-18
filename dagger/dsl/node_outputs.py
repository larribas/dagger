"""Data structures that hold information about node outputs and their usage."""

from typing import NamedTuple, Protocol, Set, runtime_checkable

from dagger.dsl.serialize import Serialize
from dagger.serializer import Serializer


@runtime_checkable
class NodeOutputReference(Protocol):
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
        """Return the outputs name of this reference."""
        ...

    @property
    def serializer(self) -> Serializer:
        """Return the serializer assigned to this output."""
        ...


class NodeOutputKeyUsage(NamedTuple):
    """
    Represents the usage of a specific key from a node output.

    The existence of this object implies that the output of a node
    has been used as such:

    ```
    @dsl.DAG
    def dag():
        f_output = f()
        g(f_output["a"])
    ```

    And therefore, the node `f` needs to contain an output of type `output.FromKey`
    """

    invocation_id: str
    output_name: str
    key_name: str
    serializer: Serializer

    def __hash__(self) -> int:
        """
        Return a hash that will be the same for two equivalent instances of this type.

        It's important to note that two tuples of this type will only be equal if all their attributes are exactly the same, but they will be equivalent if they reference the same invocation (by id) and key accessed (by name).
        """
        return hash((self.invocation_id, self.key_name))


class NodeOutputPropertyUsage(NamedTuple):
    """
    Represents the usage of a specific key from a node output.

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

    invocation_id: str
    output_name: str
    property_name: str
    serializer: Serializer

    def __hash__(self) -> int:
        """
        Return a hash that will be the same for two equivalent instances of this type.

        It's important to note that two tuples of this type will only be equal if all their attributes are exactly the same, but they will be equivalent if they reference the same invocation (by id) and property accessed (by name).
        """
        return hash((self.invocation_id, self.property_name))


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
        serialize_annotation: Serialize,
    ):
        self._invocation_id = invocation_id
        self._references: Set[NodeOutputReference] = set()
        self._serialize_annotation = serialize_annotation

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
        return self._serialize_annotation.root

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
        ref = NodeOutputPropertyUsage(
            invocation_id=self._invocation_id,
            output_name=f"property_{name}",
            property_name=name,
            serializer=self._serialize_annotation.sub_output(name)
            or self._serialize_annotation.root,
        )
        self._references.add(ref)
        return ref

    def __getitem__(self, name) -> NodeOutputReference:
        """Access a key of the output. The output must be a mapping."""
        ref = NodeOutputKeyUsage(
            invocation_id=self._invocation_id,
            output_name=f"key_{name}",
            key_name=name,
            serializer=self._serialize_annotation.sub_output(name)
            or self._serialize_annotation.root,
        )
        self._references.add(ref)
        return ref

    @property
    def references(self) -> Set[NodeOutputReference]:
        """
        Return all the ways this output has been referenced.

        If the output has never been used as input for another node, it will be empty.
        """
        return self._references

    def __repr__(self) -> str:
        """Get a human-readable string representation of this output usage."""
        return f"NodeOutputUsage(invocation_id={self._invocation_id}, references={self._references})"

    def __eq__(self, obj) -> bool:
        """Return true if both objects are equivalent."""
        return (
            isinstance(obj, NodeOutputUsage)
            and self._invocation_id == obj._invocation_id
            and self.references == obj.references
        )

    def __hash__(self) -> int:
        """
        Return a hash of the current object.

        This is used to determine whether two references are equivalent and should only be listed once in the set.
        """
        return hash((self._invocation_id))
