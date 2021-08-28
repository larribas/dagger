"""Data structures that hold information about certain elements being invoked or used throughout the definition of a DAG using the imperative DSL."""

from enum import Enum
from typing import Any, Callable, Mapping, NamedTuple, Optional, Union, get_args

from dagger.dsl.node_output_reference import NodeOutputReference
from dagger.dsl.node_output_usage import NodeOutputUsage
from dagger.dsl.parameter_usage import ParameterUsage

NodeInputReference = Union[ParameterUsage, NodeOutputReference]


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
    inputs: Mapping[str, NodeInputReference]
    output: NodeOutputUsage
    runtime_options: Optional[Mapping[str, Any]] = None
    partition_by_input: Optional[str] = None


def is_node_input_reference(obj: Any):
    """Return true if the supplied object is one of the supported node inputs."""
    return any(
        [
            isinstance(obj, supported_type)
            for supported_type in get_args(NodeInputReference)
        ]
    )
