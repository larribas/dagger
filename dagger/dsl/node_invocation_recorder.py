"""Data structures that hold information about certain elements being invoked or used throughout the definition of a DAG using the imperative DSL."""

import inspect
import uuid
from typing import Any, Callable, Mapping, Optional, Sequence

from dagger.dsl.context import node_invocations
from dagger.dsl.node_invocations import (
    NodeInputReference,
    NodeInvocation,
    NodeType,
    is_node_input_reference,
)
from dagger.dsl.node_output_partition_fan_in import NodeOutputPartitionFanIn
from dagger.dsl.node_output_partition_usage import NodeOutputPartitionUsage
from dagger.dsl.node_output_reference import NodeOutputReference
from dagger.dsl.node_output_serializer import NodeOutputSerializer
from dagger.dsl.node_output_usage import NodeOutputUsage


class NodeInvocationRecorder:
    """
    Records node invocations during the definition of a DAG via the imperative DSL.

    When an instance of this class is invoked, it records the invocation of a node in the current context (see context.py for more details).
    """

    def __init__(
        self,
        func: Callable,
        node_type: NodeType,
        serializer: NodeOutputSerializer = NodeOutputSerializer(),
        runtime_options: Mapping[str, Any] = None,
        override_id: Optional[str] = None,
    ):
        self._func = func
        self._node_type = node_type
        self._serializer = serializer
        self._runtime_options = runtime_options or {}
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
        arguments = self._bind_arguments(*args, **kwargs)
        partition_by_input = self._partition_by_input(arguments)
        self._consume_node_output_references(list(arguments.values()))

        output = NodeOutputUsage(
            invocation_id=invocation_id,
            serializer=self._serializer,
            references_node_partition=bool(partition_by_input),
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
                partition_by_input=partition_by_input,
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

    def _bind_arguments(self, *args, **kwargs) -> Mapping[str, Any]:
        sig = inspect.signature(self._func)

        try:
            bound_args = sig.bind(*args, **kwargs)
        except TypeError as e:
            raise TypeError(
                f"You have invoked the task '{self._func.__name__}' with the following arguments: args={args} kwargs={kwargs}. However, the signature of the function is '{sig}'. The following error was raised as a result of this mismatch: {e}"
            ) from e

        return {
            k: self._sanitize_argument(k, v) for k, v in bound_args.arguments.items()
        }

    def _sanitize_argument(self, name: str, arg: Any) -> Any:
        # Case 1: Fan-in of multiple outputs from a partitioned node
        if (
            isinstance(arg, Sequence)
            and len(arg) == 1
            and isinstance(arg[0], NodeOutputReference)
            and arg[0].references_node_partition
        ):
            return NodeOutputPartitionFanIn(arg[0])

        # Case 2: Mixed literals and references
        if isinstance(arg, Sequence) and any(
            [isinstance(item, NodeOutputReference) for item in arg]
        ):
            raise ValueError(
                f"Argument '{name}' of type '{type(arg).__name__}' is invalid. Arguments of this type may only contain literal/hardcoded values, or references to the same output from a partitioned node."
            )

        return arg

    def _consume_node_output_references(self, arguments: Sequence[Any]):
        """
        Explicitly mark direct outputs from other nodes as consumed.

        This is only necessary for outputs of type NodeOutputUsage.
        See the documentation of the `.consume()` function to understand why.
        """
        for arg in arguments:
            if isinstance(arg, NodeOutputReference):
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

    def _partition_by_input(self, arguments: Mapping[str, Any]) -> Optional[str]:
        partitioned_inputs = {
            k: v
            for k, v in arguments.items()
            if isinstance(v, NodeOutputPartitionUsage)
            or (isinstance(v, NodeOutputReference) and v.references_node_partition)
        }

        if len(partitioned_inputs) >= 2:
            raise ValueError(
                f"The following inputs to this node are partitioned: {sorted(list(partitioned_inputs))}. However, nodes may only be partitioned by one of their inputs. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
            )

        if partitioned_inputs:
            return next(iter(partitioned_inputs.keys()))

        return None

    def __repr__(self) -> str:
        """Get a human-readable string representation of this object."""
        return f"NodeInvocationRecorder(func={self._func}, node_type={self._node_type.value}, overridden_id={self._overridden_id}, serializer={self._serializer}, runtime_options={self._runtime_options})"

    def __eq__(self, obj) -> bool:
        """Return true if both objects are equivalent."""
        return (
            isinstance(obj, NodeInvocationRecorder)
            and self._func == obj._func
            and self._node_type == obj._node_type
            and self._overridden_id == obj._overridden_id
            and self._serializer == obj._serializer
            and self._runtime_options == obj._runtime_options
        )
