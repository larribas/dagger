from itertools import combinations

import pytest

from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromKey, FromReturnValue
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
                "invalid name": FromParam(),
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
                "invalid name": FromKey("name"),
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
                "a": FromParam(),
            },
        )

    assert (
        str(e.value)
        == "This node was declared with the following inputs: ['a']. However, the node's function has the following signature: (a, b). The inputs could not be bound to the parameters because: missing a required argument: 'b'."
    )


def test__init__partitioned_by_nonexistent_input():
    with pytest.raises(ValueError) as e:
        Task(
            lambda a, z: a + z,
            inputs={
                "z": FromParam(),
                "a": FromParam(),
            },
            partition_by_input="b",
        )

    assert (
        str(e.value)
        == "This node is partitioned by 'b'. However, 'b' is not an input of the node. The available inputs are ['a', 'z']."
    )


def test__init__with_node_partitioned_by_param():
    with pytest.raises(ValueError) as e:
        Task(
            lambda n: n,
            inputs={"n": FromParam("p")},
            partition_by_input="n",
        )

    assert (
        str(e.value)
        == "Nodes may not be partitioned by an input that comes from a parameter. This is not a valid map-reduce pattern in dagger. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
    )


def test__init__with_partitioned_node_with_partitioned_output():
    with pytest.raises(ValueError) as e:
        Task(
            lambda n: [n, n],
            inputs={
                "n": FromNodeOutput("fan-out", "nums"),
            },
            outputs={
                "more_nums": FromReturnValue(is_partitioned=True),
            },
            partition_by_input="n",
        )

    assert (
        str(e.value)
        == "Partitioned nodes may not generate partitioned outputs. This is not a valid map-reduce pattern in dagger. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
    )


#
# Properties
#


def test__inputs__cannot_be_mutated():
    task = Task(lambda x: x, inputs=dict(x=FromParam()))

    with pytest.raises(TypeError) as e:
        task.inputs["y"] = FromParam()

    assert (
        str(e.value)
        == "You may not mutate the inputs of a task. We do this to guarantee that, once initialized, the structures you build with dagger remain valid and consistent."
    )


def test__outputs__cannot_be_mutated():
    task = Task(lambda: 1, outputs=dict(x=FromReturnValue()))

    with pytest.raises(TypeError) as e:
        task.outputs["x"] = FromKey("k")

    assert (
        str(e.value)
        == "You may not mutate the outputs of a task. We do this to guarantee that, once initialized, the structures you build with dagger remain valid and consistent."
    )


def test__runtime_options__is_empty_by_default():
    task = Task(lambda: 1)
    assert len(task.runtime_options) == 0


def test__runtime_options__returns_specified_options():
    options = {"my-runtime": {"my": "options"}}
    task = Task(lambda: 1, runtime_options=options)
    assert task.runtime_options == options


def test__partition_by_input():
    assert Task(lambda: 1).partition_by_input is None
    assert (
        Task(
            lambda x: 1,
            inputs={"x": FromNodeOutput("a", "b")},
            partition_by_input="x",
        ).partition_by_input
        == "x"
    )


def test__eq():
    def f(**kwargs):
        return 11

    inputs = dict(x=FromParam())
    outputs = dict(x=FromReturnValue())
    runtime_options = {"my": "options"}

    same = [
        Task(f, inputs=inputs, outputs=outputs, runtime_options=runtime_options)
        for i in range(3)
    ]
    different = [
        Task(f, inputs=inputs, outputs=outputs, runtime_options=runtime_options),
        Task(f, inputs=inputs, outputs=outputs),
        Task(f, inputs=inputs, runtime_options=runtime_options),
        Task(f, outputs=outputs, runtime_options=runtime_options),
        Task(
            lambda **kwargs: 2,
            inputs=inputs,
            outputs=outputs,
            runtime_options=runtime_options,
        ),
    ]

    assert all(x == y for x, y in combinations(same, 2))
    assert all(x != y for x, y in combinations(different, 2))


def test__representation():
    def f(a):
        pass

    input_a = FromNodeOutput("another-node", "another-output")
    output_b = FromReturnValue()
    task = Task(
        f,
        inputs={"a": input_a},
        outputs={
            "b": output_b,
        },
        runtime_options={"my": "options"},
        partition_by_input="a",
    )

    assert (
        repr(task)
        == f"Task(func={f}, inputs={{'a': {input_a}}}, outputs={{'b': {output_b}}}, runtime_options={{'my': 'options'}}, partition_by_input=a)"
    )
