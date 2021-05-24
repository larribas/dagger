from typing import NamedTuple

import pytest

from dagger.outputs.from_property import FromProperty
from dagger.serializers import DefaultSerializer

#
# Init
#


def test__init__with_default_serializer():
    output = FromProperty("property-name")
    assert output.serializer == DefaultSerializer


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
