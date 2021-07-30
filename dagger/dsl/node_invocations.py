"""Data structures that hold information about certain elements being invoked or used throughout the definition of a DAG using the imperative DSL."""

from typing import Callable, Mapping, NamedTuple, Union

from dagger.dsl.node_outputs import NodeOutputUsage
from dagger.task import SupportedInputs as SupportedTaskInputs


class TaskInvocation(NamedTuple):
    func: Callable
    inputs: Mapping[str, SupportedTaskInputs]
    output: NodeOutputUsage


class DAGInvocation(NamedTuple):
    pass


class NodeInvocation(NamedTuple):
    id: str
    name: str
    invocation: Union[TaskInvocation, DAGInvocation]
