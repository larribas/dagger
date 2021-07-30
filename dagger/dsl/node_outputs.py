"""Data structures that hold information about node outputs and their usage."""

from typing import NamedTuple, Set


class NodeOutputReference(NamedTuple):
    """
    References the output of a node.

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

    invocation_id: str
    output_name: str


class NodeOutputUsage(NodeOutputReference):
    """
    Represents the usage of a node output.

    An instance of this class is returned whenever a task is invoked. For instance:

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
        output_name: str = "output",
    ):
        self._invocation_id = invocation_id
        self._output_name = output_name
        self._references: Set[NodeOutputReference] = set()

    @property
    def invocation_id(self) -> str:
        """
        Return the invocation id of the task that generated this output.

        The use of this property implies that the output has been used as an input in one of the other tasks, and therefore adds itself to the list of references in the NodeOutputUsage.
        """
        self._references.add(self)
        return self._invocation_id

    @property
    def output_name(self) -> str:
        """
        Return the name of this output.

        The use of this property implies that the output has been used as an input in one of the other tasks, and therefore adds itself to the list of references in the NodeOutputUsage.
        """
        self._references.add(self)
        return self._output_name

    def __getattr__(self, name) -> NodeOutputReference:
        """Access an attribute of the output. The output must be an object."""
        ref = NodeOutputReference(
            invocation_id=self._invocation_id,
            output_name=f"property_{name}",
        )
        self._references.add(ref)
        return ref

    def __getitem__(self, name) -> NodeOutputReference:
        """Access a key of the output. The output must be a mapping."""
        self._keys_accessed.add(name)
        ref = NodeOutputReference(
            invocation_id=self._invocation_id,
            output_name=f"key_{name}",
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
