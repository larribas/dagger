from dagger.output.from_return_value import FromReturnValue
from dagger.output.protocol import Output
from dagger.serializer import AsPickle, DefaultSerializer


def test__conforms_to_protocol():
    assert isinstance(FromReturnValue(), Output)


def test__serializer():
    custom_serializer = AsPickle()
    assert FromReturnValue().serializer == DefaultSerializer
    assert FromReturnValue(serializer=custom_serializer).serializer == custom_serializer


def test__is_partitioned():
    assert FromReturnValue().is_partitioned is False
    assert FromReturnValue(is_partitioned=True).is_partitioned is True


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


def test__representation():
    serializer = AsPickle()
    output = FromReturnValue(
        serializer=serializer,
        is_partitioned=True,
    )
    assert (
        repr(output)
        == f"FromReturnValue(serializer={repr(serializer)}, is_partitioned=True)"
    )
