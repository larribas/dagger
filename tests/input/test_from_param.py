from dagger.input.from_param import FromParam
from dagger.input.protocol import Input
from dagger.serializer import DefaultSerializer
from tests.input.custom_serializer import CustomSerializer


def test__conforms_to_protocol():
    assert isinstance(FromParam(), Input)


def test__with_default_serializer():
    input_ = FromParam()
    assert input_.serializer == DefaultSerializer


def test__with_custom_serializer():
    serializer = CustomSerializer()
    input_ = FromParam(serializer=serializer)
    assert input_.serializer == serializer


def test__with_default_name():
    input_ = FromParam()
    assert input_.name is None


def test__with_overridden_name():
    name = "custom-name"
    input_ = FromParam(name)
    assert input_.name == name


def test__representation():
    serializer = CustomSerializer()
    input_ = FromParam("my-param", serializer=serializer)
    assert repr(input_) == f"FromParam(name=my-param, serializer={repr(serializer)})"
