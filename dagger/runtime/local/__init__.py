"""Run DAGs or nodes in memory."""

from dagger.runtime.local.dag import invoke  # noqa
from dagger.runtime.local.types import (  # noqa
    NodeOutput,
    NodeOutputs,
    PartitionedOutput,
)
