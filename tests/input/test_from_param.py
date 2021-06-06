from dagger.input.from_param import FromParam
from dagger.input.protocol import Input
from dagger.serializer import DefaultSerializer
from tests.input.custom_serializer import CustomSerializer


def test__conforms_to_protocol():
    assert isinstance(FromParam(), Input)


def test__with_default_serializer():
    input = FromParam()
    assert input.serializer == DefaultSerializer


def test__with_custom_serializer():
    input = FromParam(serializer=CustomSerializer)
    assert input.serializer == CustomSerializer
