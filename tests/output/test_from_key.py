import pytest

from dagger.output.from_key import FromKey
from dagger.output.protocol import Output
from dagger.serializer import AsPickle, DefaultSerializer


def test__conforms_to_protocol():
    assert isinstance(FromKey("x"), Output)


def test__serializer():
    custom_serializer = AsPickle()
    assert FromKey("key-name").serializer == DefaultSerializer
    assert (
        FromKey("key-name", serializer=custom_serializer).serializer
        == custom_serializer
    )


def test__is_partitioned():
    assert FromKey("key-name").is_partitioned is False
    assert FromKey("key-name", is_partitioned=True).is_partitioned is True


#
# from_function_return_value
#


def test__from_function_return_value__with_dict__and_existing_key():
    return_value = dict(a=1, b=2)
    output = FromKey("a")
    assert output.from_function_return_value(return_value) == 1


def test__from_function_return_value__with_dict__and_missing_key():
    return_value = dict(a=1, b=2)
    output = FromKey("x")

    with pytest.raises(ValueError) as e:
        output.from_function_return_value(return_value)

    assert (
        str(e.value)
        == "An output of type FromKey('x') expects the return value of the function to be a mapping containing, at least, a key named 'x'"
    )


def test__from_function_return_value__with_unsupported_type():
    return_value = None
    output = FromKey("x")

    with pytest.raises(TypeError) as e:
        output.from_function_return_value(return_value)

    assert (
        str(e.value)
        == "This output is of type FromKey. This means we expect the return value of the function to be a mapping containing, at least, a key named 'x'"
    )


def test__representation():
    serializer = AsPickle()
    output = FromKey(
        "my-key",
        serializer=serializer,
        is_partitioned=True,
    )
    assert (
        repr(output)
        == f"FromKey(key=my-key, serializer={repr(serializer)}, is_partitioned=True)"
    )
