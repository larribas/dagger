"""Contextual information about a DAG built using the imperative DSL."""

from contextvars import ContextVar
from typing import List

from dagger.dsl.node_invocations import NodeInvocation

node_invocations: ContextVar[List[NodeInvocation]] = ContextVar("node_invocations")
