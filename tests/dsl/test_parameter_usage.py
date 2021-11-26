import pytest

from dagger.dsl.parameter_usage import ParameterUsage
from dagger.serializer import AsPickle, DefaultSerializer


def test__parameter_usage__default_properties():
    param = ParameterUsage(name="my-name")
    assert param.name == "my-name"
    assert param.serializer == DefaultSerializer


def test__parameter_usage__properties():
    param = ParameterUsage(name="my-name", serializer=AsPickle())
    assert param.name == "my-name"
    assert param.serializer == AsPickle()


def test__parameter_usage__is_not_iterable():
    param = ParameterUsage(name="my-name")

    with pytest.raises(ValueError) as e:
        for p in param:
            pass

    assert (
        str(e.value)
        == "Iterating over the value of a parameter is not a valid parallelization pattern in Dagger. You need to convert the parameter into the output of a node. Read this section in the documentation to find out more: https://larribas.me/dagger/user-guide/map-reduce/"
    )


def test__parameter_usage__representation():
    serializer = AsPickle()
    param = ParameterUsage(name="my-name", serializer=serializer)
    assert (
        repr(param)
        == f"ParameterUsage(name=my-name, default_value='EmptyDefaultValue', serializer={serializer})"
    )
