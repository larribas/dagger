import io
import os
import sys
import tempfile

import pytest

from dagger.serializer.as_json import AsJSON
from dagger.serializer.errors import DeserializationError, SerializationError
from dagger.serializer.protocol import Serializer


def test__conforms_to_protocol():
    assert isinstance(AsJSON(), Serializer)


def test_extension():
    assert AsJSON().extension == "json"


def test_serialization_and_deserialization__with_valid_values():
    serializer = AsJSON()
    valid_values = [
        None,
        1,
        1.1,
        True,
        "string",
        ["list", "of", 3],
        {"object": {"with": ["nested", "values"]}},
    ]

    with tempfile.TemporaryDirectory() as tmp:
        filename = os.path.join(tmp, "value.json")

        for value in valid_values:
            with open(filename, "wb") as writer:
                serializer.serialize(value, writer)

            with open(filename, "rb") as reader:
                deserialized_value = serializer.deserialize(reader)

            assert value == deserialized_value


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="windows uses different whitespace characters to handle indentation",
)
def test_serialization__with_indentation():
    serializer = AsJSON(indent=2)

    with tempfile.TemporaryDirectory() as tmp:
        filename = os.path.join(tmp, "value.json")

        with open(filename, "wb") as writer:
            serializer.serialize({"a": 1, "b": 2}, writer)

        with open(filename, "rb") as reader:
            serialized_value = reader.read()

        assert serialized_value == b'{\n  "a": 1,\n  "b": 2\n}'


def test_serialization__with_invalid_values():
    serializer = AsJSON()
    invalid_values = [
        float("inf"),
        float("-inf"),
        float("nan"),
        {"python", "set"},
        serializer,
    ]

    for value in invalid_values:
        with pytest.raises(SerializationError):
            serializer.serialize(value, io.BytesIO())


def test_deserialization__with_invalid_values():
    serializer = AsJSON()
    invalid_values = [
        b"}{",
        b"",
        b'"a"1',
    ]

    for value in invalid_values:
        with pytest.raises(DeserializationError):
            serializer.deserialize(io.BytesIO(value))
