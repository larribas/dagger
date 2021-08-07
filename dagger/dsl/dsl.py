"""Define DAGs through an imperative domain-specific language."""

from typing import Callable

from dagger.dag import DAG as DeclarativeDAG
from dagger.dsl.dag_builder import DAGBuilder
from dagger.dsl.node_invocation_recorder import NodeInvocationRecorder
from dagger.dsl.node_invocations import NodeType
from dagger.dsl.node_outputs import NodeOutputUsage

_HELP_COMMENT = """
You can check examples of how to use the DSL in the examples/dsl directory.
"""


class DAGInvocationRecorderAndBuilder:
    """Multiplexes the functionality of a function decorated with `@dsl.DAG` so that it can act as both a NodeInvocationRecorder (and thus invoked from within other DAGs) and as a DAGBuilder (and thus build a DAG data structure when invoking `.build()` on the decorated function)."""

    def __init__(self, func: Callable):
        self._recorder = NodeInvocationRecorder(func, node_type=NodeType.DAG)
        self._builder = DAGBuilder(func)

    def __call__(self, *args, **kwargs) -> NodeOutputUsage:
        return self._recorder(*args, **kwargs)

    def build(self) -> DeclarativeDAG:
        return self._builder.build()


def DAG(func: Callable):
    f"""
    Decorate a function that builds a DAG using the imperative DSL.

    {_HELP_COMMENT}
    """
    return DAGInvocationRecorderAndBuilder(func)


def task(func: Callable):
    f"""
    Decorate a function that runs a task using the imperative DSL.

    {_HELP_COMMENT}
    """
    return NodeInvocationRecorder(func, node_type=NodeType.TASK)
