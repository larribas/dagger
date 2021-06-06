import pytest

from dagger.output.from_key import FromKey
from dagger.output.protocol import Output
from dagger.serializer import DefaultSerializer


def test__conforms_to_protocol():
    assert isinstance(FromKey("x"), Output)


#
# Init
#


def test__init__with_default_serializer():
    output = FromKey("key-name")
    assert output.serializer == DefaultSerializer


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
