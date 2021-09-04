"""Protocol all serializers should conform to."""

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Serializer(Protocol):  # pragma: no cover
    """Protocol all serializers should conform to."""

    extension: str

    def serialize(self, value: Any) -> bytes:
        """Serialize a value into a sequence of bytes."""
        ...

    def deserialize(self, serialized_value: bytes) -> Any:
        """Deserialize a sequence of bytes into a value."""
        ...
