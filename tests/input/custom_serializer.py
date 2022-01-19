# noqa
from typing import Any, BinaryIO


class CustomSerializer:
    """Custom serializer implementation to test the injection of different serialization strategies to an input."""

    @property
    def extension(self) -> str:  # noqa
        return "ext"

    def serialize(self, value: Any, writer: BinaryIO):  # noqa
        raise NotImplementedError()

    def deserialize(self, reader: BinaryIO) -> Any:  # noqa
        raise NotImplementedError()

    def __repr__(self) -> str:  # noqa
        return "CustomSerializerInstance"
