from typing import Any, Protocol

from dagger.serializers.errors import DeserializationError, SerializationError
from dagger.serializers.json import JSON


class Serializer(Protocol):
    extension: str

    def serialize(self, value: Any) -> bytes:
        ...

    def deserialize(self, serialized_value: bytes) -> Any:
        ...


# TODO: Make the default a Pickle serializer
DefaultSerializer = JSON()


__all__ = [
    Serializer.__name__,
    SerializationError.__name__,
    DeserializationError.__name__,
    # Out-of-the-box serializers
    "DefaultSerializer",
    JSON.__name__,
]
