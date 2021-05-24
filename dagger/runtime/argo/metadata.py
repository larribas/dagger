from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class Metadata:
    name: str
    generate_name_from_prefix: bool = False
    namespace: Optional[str] = None
    annotations: Dict[str, str] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=list)
    # TODO: Validate names and namespaces upon initialization


def object_meta(metadata: Metadata) -> dict:
    """
    Returns a minimal representation of an ObjectMeta with the supplied information
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#objectmeta
    """
    meta = {}

    if metadata.generate_name_from_prefix:
        meta["generateName"] = metadata.name
    else:
        meta["name"] = metadata.name

    if metadata.annotations:
        meta["annotations"] = metadata.annotations

    if metadata.labels:
        meta["labels"] = metadata.labels

    if metadata.namespace:
        meta["namespace"] = metadata.namespace

    return meta
