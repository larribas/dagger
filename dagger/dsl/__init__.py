"""Define DAGs through an imperative domain-specific language."""

from typing import Callable

from dagger.dsl.dag_builder import DAGBuilder
from dagger.dsl.task_invocation_recorder import TaskInvocationRecorder

_HELP_COMMENT = """
You can check examples of how to use the DSL in the examples/dsl directory.
"""


def DAG(build_func: Callable):
    f"""
    Decorate a function that builds a DAG using the imperative DSL.

    {_HELP_COMMENT}
    """
    return DAGBuilder(build_func)


def task(func: Callable):
    f"""
    Decorate a function that runs a task using the imperative DSL.

    {_HELP_COMMENT}
    """
    return TaskInvocationRecorder(func)
