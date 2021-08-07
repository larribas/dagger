"""Data structures that hold information about certain elements being invoked or used throughout the definition of a DAG using the imperative DSL."""

import inspect
import uuid
from typing import Callable, Mapping, Optional

from dagger.dsl.context import node_invocations
from dagger.dsl.errors import NodeInvokedWithMismatchedArgumentsError
from dagger.dsl.node_invocations import NodeInvocation, NodeType, SupportedNodeInput
from dagger.dsl.node_outputs import NodeOutputUsage


class NodeInvocationRecorder:
    """
    Records node invocations during the definition of a DAG via the imperative DSL.

    When an instance of this class is invoked, it records the invocation of a node in the current context (see context.py for more details).
    """

    def __init__(
        self,
        func: Callable,
        node_type: NodeType,
        override_id: Optional[str] = None,
    ):
        self._func = func
        self._node_type = node_type
        self._overridden_id = override_id

    def __call__(self, *args, **kwargs) -> NodeOutputUsage:
        """
        Record the invocation of this node with a series of arguments.

        Returns
        -------
        A representation of the node's output.

        Raises
        ------
        NodeInvokedWithMismatchedArguments
            When the arguments the node is invoked with do not match the arguments declared in the function's signature.

        NotImplementedError
            When some of the input/output types supported by dagger are not yet supported by the imperative DSL.
        """
        invocation_id = self._overridden_id or uuid.uuid4().hex
        inputs = self._bind_arguments(*args, **kwargs)
        self._consume_node_output_references(inputs)
        output = NodeOutputUsage(invocation_id=invocation_id)

        invocations = node_invocations.get([])
        invocations.append(
            NodeInvocation(
                id=invocation_id,
                name=self._func.__name__.replace("_", "-"),
                node_type=self._node_type,
                func=self._func,
                inputs=inputs,
                output=output,
            ),
        )
        node_invocations.set(invocations)

        return output

    @property
    def func(self):
        """Return the function recorded by this class."""
        return self._func

    @property
    def node_type(self):
        """Return the node type associated with this class."""
        return self._node_type

    def _bind_arguments(self, *args, **kwargs) -> Mapping[str, SupportedNodeInput]:
        sig = inspect.signature(self._func)

        try:
            bound_args = sig.bind(*args, **kwargs)
        except TypeError as e:
            raise NodeInvokedWithMismatchedArgumentsError(
                f"You have invoked the task '{self._func.__name__}' with the following arguments: args={args} kwargs={kwargs}. However, the signature of the function is '{sig}'. The following error was raised as a result of this mismatch: {e}"
            )

        return bound_args.arguments

    def _consume_node_output_references(self, inputs: Mapping[str, SupportedNodeInput]):
        """
        Explicitly mark direct outputs from other nodes as consumed.

        This is only necessary for outputs of type NodeOutputUsage.
        See the documentation of the `.consume()` function to understand why.
        """
        for i in inputs.values():
            if isinstance(i, NodeOutputUsage):
                i.consume()
