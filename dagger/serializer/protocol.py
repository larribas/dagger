"""Protocol all serializers should conform to."""

import io
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Serializer(Protocol):  # pragma: no cover
    """Protocol all serializers should conform to."""

    extension: str

    def serialize(self, value: Any, writer: io.BufferedWriter):
        """Serialize a value and write it to the provided writer stream."""
        ...

    def deserialize(self, reader: io.BufferedReader) -> Any:
        """Deserialize a stream of bytes into a value."""
        ...
