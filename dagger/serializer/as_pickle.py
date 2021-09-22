"""Serialization strategy based on the Pickle protocol."""

from typing import Any, BinaryIO

from dagger.serializer.errors import DeserializationError, SerializationError


class AsPickle:
    """
    Serializer implementation that uses Pickle to marshal/unmarshal Python data structures.

    Reference: https://docs.python.org/3/library/pickle.html
    """

    extension = "pickle"

    def serialize(self, value: Any, writer: BinaryIO):
        """Serialize a value using the Pickle protocol."""
        import pickle

        try:
            pickle.dump(value, writer)
        except (pickle.PicklingError, AttributeError) as e:
            raise SerializationError(e)

    def deserialize(self, reader: BinaryIO) -> Any:
        """Deserialize a pickled object into the value it represents."""
        import pickle

        try:
            return pickle.load(reader)
        except (
            pickle.UnpicklingError,
            AttributeError,
            EOFError,
            ImportError,
            IndexError,
            TypeError,
        ) as e:
            raise DeserializationError(e)

    def __repr__(self) -> str:
        """Get a human-readable string representation of the serializer."""
        return "AsPickle()"

    def __eq__(self, obj) -> bool:
        """Return true if both serializers are equivalent."""
        return isinstance(obj, AsPickle)
