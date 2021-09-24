import io
import os
import tempfile

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

    with tempfile.TemporaryDirectory() as tmp:
        filename = os.path.join(tmp, "value.pickle")

        for value in valid_values:
            with open(filename, "wb") as writer:
                serializer.serialize(value, writer)

            with open(filename, "rb") as reader:
                deserialized_value = serializer.deserialize(reader)

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
            serializer.serialize(value, io.BytesIO())


def test_deserialization__with_invalid_values():
    serializer = AsPickle()
    invalid_values = [
        b"",
        b"arbitrary byte string",
    ]

    for value in invalid_values:
        with pytest.raises(DeserializationError):
            serializer.deserialize(io.BytesIO(value))
