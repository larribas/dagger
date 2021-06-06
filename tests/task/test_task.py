import pytest

import dagger.input as input
import dagger.output as output
from dagger.serializer import DefaultSerializer
from dagger.task import Task

#
# Init
#


def test__init__with_an_invalid_input_name():
    with pytest.raises(ValueError):
        Task(
            lambda: 1,
            inputs={
                "invalid name": input.FromParam(),
            },
        )


def test__init__with_an_unsupported_input():
    class UnsupportedInput:
        def __init__(self):
            self.serializer = DefaultSerializer

    with pytest.raises(TypeError) as e:
        Task(
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
        Task(
            lambda: 1,
            outputs={
                "invalid name": output.FromKey("name"),
            },
        )


def test__init__with_an_unsupported_output():
    class UnsupportedOutput:
        def __init__(self):
            self.serializer = DefaultSerializer

    def from_function_return_value(self, results):
        return results

    with pytest.raises(TypeError) as e:
        Task(
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
        Task(
            f,
            inputs={
                "a": input.FromParam(),
            },
        )

    assert (
        str(e.value)
        == "This node was declared with the following inputs: ['a']. However, the node's function has the following signature: (a, b). The inputs could not be bound to the parameters because: missing a required argument: 'b'"
    )
