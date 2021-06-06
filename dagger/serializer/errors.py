"""Potential errors that may occur when serializing/deserializing values."""


class SerializationError(Exception):
    """Error when attempting to serialize a value with the specified strategy."""

    pass


class DeserializationError(Exception):
    """Error when attempting to deserialize a value with the specified strategy."""

    pass
