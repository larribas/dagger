"""Define DAGs through an imperative domain-specific language."""

from typing import Any, Callable, Mapping

from dagger.dsl.node_invocation_recorder import NodeInvocationRecorder
from dagger.dsl.node_invocations import NodeType
from dagger.dsl.node_output_serializer import NodeOutputSerializer


def DAG(
    runtime_options: Mapping[str, Any] = None,
) -> Callable[[Callable], NodeInvocationRecorder]:
    """
    Decorate a function as a DAG.

    You can check examples of how to use the DSL in the examples/dsl directory.
    """

    def decorator(func: Callable) -> NodeInvocationRecorder:
        return NodeInvocationRecorder(
            func,
            node_type=NodeType.DAG,
            runtime_options=runtime_options,
        )

    return decorator


def task(
    serializer: NodeOutputSerializer = NodeOutputSerializer(),
    runtime_options: Mapping[str, Any] = None,
) -> Callable[[Callable], NodeInvocationRecorder]:
    """
    Decorate a function as a Task.

    You can check examples of how to use the DSL in the examples/dsl directory.
    """
    runtime_options = runtime_options or {}

    def decorator(func: Callable) -> NodeInvocationRecorder:
        return NodeInvocationRecorder(
            func,
            node_type=NodeType.TASK,
            serializer=serializer,
            runtime_options=runtime_options,
        )

    return decorator
