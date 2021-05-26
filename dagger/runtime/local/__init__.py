"""Run DAGs or nodes in memory."""
from dagger.runtime.local.dag import invoke_dag
from dagger.runtime.local.node import invoke_node

__all__ = [
    "invoke_dag",
    "invoke_node",
]
