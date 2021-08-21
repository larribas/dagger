"""Data types used for local invocations."""

from typing import Any, List, Mapping, Union

#: A series of partitions
Partitioned = List

#: One of the outputs of a node, which may be partitioned
NodeOutput = Union[bytes, Partitioned[bytes]]

#: All outputs of a node indexed by their name. Node executions may be partitioned, in which case this is a list.
NodeOutputs = Mapping[str, NodeOutput]

#: All executions of a node. If the node is partitioned there will only be one. Otherwise, there may be many.
NodeExecutions = Union[NodeOutputs, Partitioned[NodeOutputs]]

#: The parameters supplied to a node (plain, not serialized)
NodeParams = Mapping[str, Any]
