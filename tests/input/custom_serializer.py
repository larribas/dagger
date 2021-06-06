# noqa


class CustomSerializer:
    """Custom serializer implementation to test the injection of different serialization strategies to an input."""

    @property
    def extension(self) -> str:  # noqa
        return "ext"

    def serialize(self, value: str) -> bytes:  # noqa
        return b"serialized"

    def deserialize(self, serialized_value: bytes) -> str:  # noqa
        return "deserialized"
