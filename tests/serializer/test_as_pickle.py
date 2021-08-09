import pytest

from dagger.serializer.as_pickle import AsPickle
from dagger.serializer.errors import DeserializationError, SerializationError
from dagger.serializer.protocol import Serializer


def test__conforms_to_protocol():
    assert isinstance(AsPickle(), Serializer)


def test_extension():
    assert AsPickle().extension == "pickle"


def test_serialization_and_deserialization__with_valid_values():
    serializer = AsPickle()
    valid_values = [
        None,
        1,
        1.1,
        True,
        "string",
        ["list", "of", 3],
        {"object": {"with": ["nested", "values"]}},
        {"python", "set"},
        float("inf"),
        float("-inf"),
        serializer,
    ]

    for value in valid_values:
        serialized_value = serializer.serialize(value)
        assert (type(serialized_value)) == bytes

        deserialized_value = serializer.deserialize(serialized_value)
        assert value == deserialized_value


def test_serialization__with_invalid_values():
    class ClassOutOfScope:
        pass

    def func_out_of_scope():
        pass

    serializer = AsPickle()
    invalid_values = [
        lambda: 1,
        ClassOutOfScope,
        ClassOutOfScope(),
        func_out_of_scope,
    ]

    for value in invalid_values:
        with pytest.raises(SerializationError):
            serializer.serialize(value)


def test_deserialization__with_invalid_values():
    serializer = AsPickle()
    invalid_values = [
        b"arbitrary byte string",
        {"python": ["data", "structure"]},
        serializer,
    ]

    for value in invalid_values:
        with pytest.raises(DeserializationError):
            serializer.deserialize(value)
