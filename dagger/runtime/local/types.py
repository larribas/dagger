"""Data types used for local invocations."""

from typing import Any, List, Mapping, Union

NodeOutput = Union[bytes, List[bytes]]
NodeParams = Mapping[str, Any]
NodePartitions = Union[
    Mapping[str, NodeOutput],
    List[Mapping[str, NodeOutput]],
]
