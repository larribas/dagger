"""Run DAGs or nodes in memory."""

from dagger.runtime.local.invoke import (  # noqa
    ReturnDeserializedOutputs,
    StoreSerializedOutputsInPath,
    invoke,
)
from dagger.runtime.local.types import (  # noqa
    NodeOutput,
    NodeOutputs,
    OutputFile,
    PartitionedOutput,
)
