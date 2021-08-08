"""Define DAGs through an imperative domain-specific language."""

from typing import Callable

from dagger.dsl.node_invocation_recorder import NodeInvocationRecorder
from dagger.dsl.node_invocations import NodeType


def DAG(func: Callable):
    """
    Decorate a function that builds a DAG using the imperative DSL.

    You can check examples of how to use the DSL in the examples/dsl directory.
    """
    return NodeInvocationRecorder(func, node_type=NodeType.DAG)


def task(func: Callable):
    """
    Decorate a function that runs a task using the imperative DSL.

    You can check examples of how to use the DSL in the examples/dsl directory.
    """
    return NodeInvocationRecorder(func, node_type=NodeType.TASK)
