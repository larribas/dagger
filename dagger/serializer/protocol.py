"""Protocol all serializers should conform to."""

from typing import Any, BinaryIO, Protocol, runtime_checkable


@runtime_checkable
class Serializer(Protocol):  # pragma: no cover
    """Protocol all serializers should conform to."""

    extension: str

    def serialize(self, value: Any, writer: BinaryIO):
        """Serialize a value and write it to the provided writer stream."""
        ...

    def deserialize(self, reader: BinaryIO) -> Any:
        """Deserialize a stream of bytes into a value."""
        ...
