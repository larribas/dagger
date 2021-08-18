"""Data structures that hold information about certain elements being invoked or used throughout the definition of a DAG using the imperative DSL."""

import inspect
import uuid
from typing import Any, Callable, Mapping, Optional, Sequence

from dagger.dsl.context import node_invocations
from dagger.dsl.errors import NodeInvokedWithMismatchedArgumentsError
from dagger.dsl.node_invocations import (
    NodeInputReference,
    NodeInvocation,
    NodeType,
    is_node_input_reference,
)
from dagger.dsl.node_outputs import NodeOutputUsage
from dagger.dsl.serialize import Serialize, find_serialize_annotation


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
        self._runtime_options: Mapping[str, Any] = {}

    def with_runtime_options(
        self, runtime_options: Mapping[str, Any]
    ) -> "NodeInvocationRecorder":
        """Set arbitrary runtime options for this node."""
        self._runtime_options = runtime_options
        return self

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
        arguments = self._bind_arguments(*args, **kwargs)
        self._consume_node_output_references(list(arguments.values()))
        output = NodeOutputUsage(
            invocation_id=invocation_id,
            serialize_annotation=find_serialize_annotation(self._func) or Serialize(),
        )

        invocations = node_invocations.get([])
        invocations.append(
            NodeInvocation(
                id=invocation_id,
                name=self._func.__name__.replace("_", "-"),
                node_type=self._node_type,
                func=self._func_with_preset_params(arguments),
                inputs=self._inputs(arguments),
                output=output,
                runtime_options=self._runtime_options,
            ),
        )
        node_invocations.set(invocations)

        return output

    @property
    def func(self) -> Callable:
        """Return the function recorded by this class."""
        return self._func

    @property
    def runtime_options(self) -> Mapping[str, Any]:
        """Return the runtime options associated with this class."""
        return self._runtime_options

    @property
    def node_type(self) -> NodeType:
        """Return the node type associated with this class."""
        return self._node_type

    def _bind_arguments(self, *args, **kwargs) -> Mapping[str, Any]:
        sig = inspect.signature(self._func)

        try:
            bound_args = sig.bind(*args, **kwargs)
        except TypeError as e:
            raise NodeInvokedWithMismatchedArgumentsError(
                f"You have invoked the task '{self._func.__name__}' with the following arguments: args={args} kwargs={kwargs}. However, the signature of the function is '{sig}'. The following error was raised as a result of this mismatch: {e}"
            )

        return bound_args.arguments

    def _consume_node_output_references(self, arguments: Sequence[Any]):
        """
        Explicitly mark direct outputs from other nodes as consumed.

        This is only necessary for outputs of type NodeOutputUsage.
        See the documentation of the `.consume()` function to understand why.
        """
        for arg in arguments:
            if isinstance(arg, NodeOutputUsage):
                arg.consume()

    def _func_with_preset_params(self, arguments: Mapping[str, Any]) -> Callable:
        """
        Return the function where all the arguments that come from a literal value (instead of a reference built by the DSL) are preset and don't need to be injected as a parameter anymore.

        For instance, given:

        ```
        @dsl.task
        def f(a, b, c):
            pass

        @dsl.DAG
        def dsl(a):
            f(a=a, b=2, c=3)
        ```

        This function would return a function f' so that f'(a) == f(a, 2, 3).
        """
        preset_params = {}
        for argument_name, argument_value in arguments.items():
            if not is_node_input_reference(argument_value):
                preset_params[argument_name] = argument_value

        if preset_params:
            return lambda *args, **kwargs: self._func(
                *args, **{**kwargs, **preset_params}
            )

        return self._func

    def _inputs(self, arguments: Mapping[str, Any]) -> Mapping[str, NodeInputReference]:
        """Filter the node input references that come as arguments to the node invocation."""
        return {
            argument_name: argument_value
            for argument_name, argument_value in arguments.items()
            if is_node_input_reference(argument_value)
        }

    def __repr__(self) -> str:
        """Get a human-readable string representation of this object."""
        return f"NodeInvocationRecorder(func={self._func}, node_type={self._node_type}, overridden_id={self._overridden_id}, runtime_options={self._runtime_options})"

    def __eq__(self, obj) -> bool:
        """Return true if both objects are equivalent."""
        return (
            isinstance(obj, NodeInvocationRecorder)
            and self._func == obj._func
            and self._node_type == obj._node_type
            and self._overridden_id == obj._overridden_id
            and self._runtime_options == obj._runtime_options
        )
