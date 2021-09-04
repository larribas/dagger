from typing import NamedTuple

import pytest

from dagger.output.from_property import FromProperty
from dagger.output.protocol import Output
from dagger.serializer import AsPickle, DefaultSerializer


def test__conforms_to_protocol():
    assert isinstance(FromProperty("x"), Output)


def test__serializer():
    custom_serializer = AsPickle()
    assert FromProperty("property-name").serializer == DefaultSerializer
    assert (
        FromProperty("property-name", serializer=custom_serializer).serializer
        == custom_serializer
    )


def test__is_partitioned():
    assert FromProperty("property-name").is_partitioned is False
    assert FromProperty("property-name", is_partitioned=True).is_partitioned is True


#
# from_function_return_value
#


class ReturnValue(NamedTuple):
    a: int
    b: int


def test__from_function_return_value__with_existing_property():
    return_value = ReturnValue(a=1, b=2)
    output = FromProperty("a")
    assert output.from_function_return_value(return_value) == 1


def test__from_function_return_value__with_missing_property():
    return_value = None
    output = FromProperty("x")

    with pytest.raises(TypeError) as e:
        output.from_function_return_value(return_value)

    assert (
        str(e.value)
        == "This output is of type FromProperty. This means we expect the return value of the function to be an object with a property named 'x'"
    )


def test__representation():
    serializer = AsPickle()
    output = FromProperty(
        "my-property",
        serializer=serializer,
        is_partitioned=True,
    )
    assert (
        repr(output)
        == f"FromProperty(name=my-property, serializer={repr(serializer)}, is_partitioned=True)"
    )
