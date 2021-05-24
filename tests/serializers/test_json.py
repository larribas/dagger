import pytest

from dagger.serializers import JSON, DeserializationError, SerializationError


def test_extension():
    assert JSON().extension == "json"


def test_serialization_and_deserialization__with_valid_values():
    serializer = JSON()
    valid_values = [
        None,
        1,
        1.1,
        True,
        "string",
        ["list", "of", 3],
        {"object": {"with": ["nested", "values"]}},
    ]

    for value in valid_values:
        serialized_value = serializer.serialize(value)
        assert (type(serialized_value)) == bytes

        deserialized_value = serializer.deserialize(serialized_value)
        assert value == deserialized_value


def test_serialization__with_indentation():
    serializer = JSON(indent=2)
    serialized_value = serializer.serialize({"a": 1, "b": 2})
    assert serialized_value == b'{\n  "a": 1,\n  "b": 2\n}'


def test_serialization__with_invalid_values():
    serializer = JSON()
    invalid_values = [
        float("inf"),
        float("-inf"),
        float("nan"),
        {"python", "set"},
        serializer,
    ]

    for value in invalid_values:
        with pytest.raises(SerializationError):
            serializer.serialize(value)


def test_deserialization__with_invalid_values():
    serializer = JSON()
    invalid_values = [
        {"python": ["data", "structure"]},
        serializer,
    ]

    for value in invalid_values:
        with pytest.raises(DeserializationError):
            serializer.deserialize(value)
