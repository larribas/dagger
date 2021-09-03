"""Metadata for Argo CRDs."""

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional


@dataclass(frozen=True)
class Metadata:
    """Metadata that may be provided for an Argo CRD."""

    name: str
    generate_name_from_prefix: bool = False
    namespace: Optional[str] = None
    annotations: Mapping[str, str] = field(default_factory=dict)
    labels: Mapping[str, str] = field(default_factory=dict)


def object_meta(metadata: Metadata) -> Mapping[str, Any]:
    """
    Return a minimal representation of an ObjectMeta with the supplied information.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#objectmeta
    """
    meta: Dict[str, Any] = {}

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
