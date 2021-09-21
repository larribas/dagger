import pytest

from dagger import DeserializationError, SerializationError, Serializer
from docs.code_snippets.yaml_serializer import AsYAML


def test__conforms_to_protocol():
    assert isinstance(AsYAML(), Serializer)


def test_serialize_valid_values():
    serializer = AsYAML()
    valid_values = [
        None,
        1,
        2.3,
        True,
        "string",
        [],
        [1, "two", 3],
        {},
        {"one": 2, "three": [4, "five"]},
    ]

    for value in valid_values:
        serialized_value = serializer.serialize(value)
        deserialized_value = serializer.deserialize(serialized_value)
        assert value == deserialized_value


def test_serialize_invalid_values():
    class CustomType:
        pass

    serializer = AsYAML()
    invalid_values = [
        CustomType,
        CustomType(),
    ]

    for value in invalid_values:
        with pytest.raises(SerializationError):
            serializer.serialize(value)


def test_deserialize_invalid_values():
    serializer = AsYAML()
    invalid_values = [
        b"\x04",
        b"}{",
        b"a: [b, ]c],",
    ]

    for value in invalid_values:
        with pytest.raises(DeserializationError):
            serializer.deserialize(value)
