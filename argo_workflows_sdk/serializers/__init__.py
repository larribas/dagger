from typing import Any, Protocol

from argo_workflows_sdk.serializers.errors import (
    DeserializationError,
    SerializationError,
)
from argo_workflows_sdk.serializers.json import JSON


class Serializer(Protocol):
    extension: str

    def serialize(self, value: Any) -> bytes:
        ...

    def deserialize(self, serialized_value: bytes) -> Any:
        ...


# TODO: Make the default a Pickle serializer
DefaultSerializer = JSON()


__all__ = [
    Serializer,
    SerializationError,
    DeserializationError,
    # Out-of-the-box serializers
    DefaultSerializer,
    JSON,
]
