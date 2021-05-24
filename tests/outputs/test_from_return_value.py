import pytest

from dagger.outputs.from_return_value import FromReturnValue
from dagger.serializers import DefaultSerializer

#
# Init
#


def test__init__with_default_serializer():
    output = FromReturnValue()
    assert output.serializer == DefaultSerializer


#
# from_function_return_value
#


def test__from_function_return_value__is_the_identity_function():
    return_values = [
        None,
        1,
        dict(a=1, b=2),
        lambda: 2,
    ]

    output = FromReturnValue()

    for return_value in return_values:
        assert output.from_function_return_value(return_value) == return_value
