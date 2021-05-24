import pytest

import dagger.inputs as inputs
import dagger.outputs as outputs
from dagger.node import Node, validate_name
from dagger.serializers import DefaultSerializer


class UnsupportedInput:
    def __init__(self):
        self.serializer = DefaultSerializer


class UnsupportedOutput:
    def __init__(self):
        self.serializer = DefaultSerializer

    def from_function_return_value(self, results):
        return results


#
# Init
#


def test__init__with_an_invalid_input_name():
    with pytest.raises(ValueError):
        Node(
            lambda: 1,
            inputs={
                "invalid name": inputs.FromParam(),
            },
        )


def test__init__with_an_unsupported_input():
    with pytest.raises(ValueError) as e:
        Node(
            lambda x: 1,
            inputs={
                "x": UnsupportedInput(),
            },
        )

    assert (
        str(e.value)
        == "Input 'x' is of type 'UnsupportedInput'. However, nodes only support the following types of inputs: ['FromParam', 'FromNodeOutput']"
    )


def test__init__with_an_invalid_output_name():
    with pytest.raises(ValueError):
        Node(
            lambda: 1,
            outputs={
                "invalid name": outputs.FromKey("name"),
            },
        )


def test__init__with_an_unsupported_output():
    with pytest.raises(ValueError) as e:
        Node(
            lambda: 1,
            outputs={
                "x": UnsupportedOutput(),
            },
        )

    assert (
        str(e.value)
        == "Output 'x' is of type 'UnsupportedOutput'. However, nodes only support the following types of outputs: ['FromReturnValue', 'FromKey', 'FromProperty']"
    )


def test__init__with_input_and_signature_mismatch():
    def f(a, b):
        pass

    with pytest.raises(TypeError) as e:
        Node(
            f,
            inputs={
                "a": inputs.FromParam(),
            },
        )

    assert (
        str(e.value)
        == "This node was declared with the following inputs: ['a']. However, the node's function has the following signature: (a, b). The inputs could not be bound to the parameters because: missing a required argument: 'b'"
    )


#
# validate_name
#


def test__validate_name__with_valid_names():
    valid_names = [
        "param",
        "name-with-dashes",
        "name-with-dashes-and-123",
        "x" * 64,
    ]

    for name in valid_names:
        # We are testing it doesn't raise any validation errors
        validate_name(name)


def test__validate_name__with_invalid_names():
    invalid_names = [
        "",
        "name with spaces",
        "x" * 65,
        "with$ymßols",
        "with_underscores",
    ]

    for name in invalid_names:
        with pytest.raises(ValueError) as e:
            validate_name(name)

        assert (
            str(e.value)
            == f"'{name}' is not a valid name for a node. Node names must comply with the regex ^[a-zA-Z0-9][a-zA-Z0-9-]{{0,63}}$"
        )
