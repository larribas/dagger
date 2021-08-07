"""Data structures that hold information about certain elements being invoked or used throughout the definition of a DAG using the imperative DSL."""

from enum import Enum
from typing import Callable, Mapping, NamedTuple, Union

from dagger.dsl.node_outputs import NodeOutputReference, NodeOutputUsage
from dagger.dsl.parameter_usage import ParameterUsage

SupportedNodeInput = Union[ParameterUsage, NodeOutputReference]


class NodeType(Enum):
    """Define the type of a node invocation."""

    DAG = "dag"
    TASK = "task"


class NodeInvocation(NamedTuple):
    """Represents the invocation of a node in the context of a DAG definition."""

    id: str
    name: str
    node_type: NodeType
    func: Callable
    inputs: Mapping[str, SupportedNodeInput]
    output: NodeOutputUsage
