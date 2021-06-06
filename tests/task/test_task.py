import pytest

import dagger.input as input
import dagger.output as output
from dagger.serializer import DefaultSerializer
from dagger.task import Task

#
# Initialization
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


#
# Properties
#


def test__inputs__cannot_be_mutated():
    task = Task(lambda x: x, inputs=dict(x=input.FromParam()))

    with pytest.raises(TypeError) as e:
        task.inputs["y"] = input.FromParam()

    assert (
        str(e.value)
        == "You may not mutate the inputs of a task. We do this to guarantee that, once initialized, the structures you build with dagger remain valid and consistent."
    )


def test__outputs__cannot_be_mutated():
    task = Task(lambda: 1, outputs=dict(x=output.FromReturnValue()))

    with pytest.raises(TypeError) as e:
        task.outputs["x"] = output.FromKey("k")

    assert (
        str(e.value)
        == "You may not mutate the outputs of a task. We do this to guarantee that, once initialized, the structures you build with dagger remain valid and consistent."
    )


def test__runtime_options__is_empty_by_default():
    task = Task(lambda: 1)
    assert len(task.runtime_options) == 0


def test__runtime_options__returns_specified_options():
    options = {"some", "arbitrary", "options"}
    task = Task(lambda: 1, runtime_options=options)
    assert task.runtime_options == options


def test__runtime_options__cannot_be_mutated():
    task = Task(lambda: 1)

    with pytest.raises(AttributeError):
        task.runtime_options.append("extra-option")
