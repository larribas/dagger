import tempfile
import warnings

import pytest

from dagger.input import FromParam
from dagger.output import FromKey, FromReturnValue
from dagger.runtime.local.output import deserialized_outputs
from dagger.runtime.local.task import invoke_task
from dagger.serializer import AsJSON, SerializationError
from dagger.task import Task


def test__invoke_task__without_inputs_or_outputs():
    invocations = []
    task = Task(lambda: invocations.append(1))

    with tempfile.TemporaryDirectory() as tmp:
        assert invoke_task(task, params={}, output_path=tmp) == {}

    assert invocations == [1]


def test__invoke_task__with_single_input_and_output():
    task = Task(
        lambda number: number * 2,
        inputs=dict(number=FromParam()),
        outputs=dict(doubled_number=FromReturnValue()),
    )

    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_task(
            task,
            params={"number": 2},
            output_path=tmp,
        )
        assert deserialized_outputs(outputs) == {"doubled_number": 4}


def test__invoke_task__with_multiple_inputs_and_outputs():
    task = Task(
        lambda first_name, last_name: dict(
            message=f"Hello {first_name} {last_name}",
            name_length=len(first_name) + len(last_name),
        ),
        inputs=dict(
            first_name=FromParam(),
            last_name=FromParam(),
        ),
        outputs=dict(
            message=FromKey("message"),
            name_length=FromKey("name_length"),
        ),
    )
    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_task(
            task,
            params={"first_name": "John", "last_name": "Doe"},
            output_path=tmp,
        )
        assert deserialized_outputs(outputs) == {
            "message": "Hello John Doe",
            "name_length": 7,
        }


def test__invoke_task__with_missing_input_parameter():
    task = Task(
        lambda a, b: 1,
        inputs=dict(
            a=FromParam(),
            b=FromParam(),
        ),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.TemporaryDirectory() as tmp:
            invoke_task(task, params={}, output_path=tmp)

    assert (
        str(e.value)
        == "The following parameters are required by the task but were not supplied: ['a', 'b']"
    )


def test__invoke_task__with_mismatched_outputs():
    task = Task(lambda: 1, outputs=dict(a=FromKey("x")))
    with pytest.raises(TypeError) as e:
        with tempfile.TemporaryDirectory() as tmp:
            invoke_task(task, params={}, output_path=tmp)

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: This output is of type FromKey. This means we expect the return value of the function to be a mapping containing, at least, a key named 'x'"
    )


def test__invoke_task__with_missing_outputs():
    task = Task(lambda: dict(a=1), outputs=dict(x=FromKey("x")))
    with pytest.raises(ValueError) as e:
        with tempfile.TemporaryDirectory() as tmp:
            invoke_task(task, params={}, output_path=tmp)

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: An output of type FromKey('x') expects the return value of the function to be a mapping containing, at least, a key named 'x'"
    )


def test__invoke_task__with_unserializable_outputs():
    task = Task(lambda: dict(a=lambda: 2), outputs=dict(x=FromKey("a")))
    with pytest.raises(SerializationError) as e:
        with tempfile.TemporaryDirectory() as tmp:
            invoke_task(task, params={}, output_path=tmp)

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: Object of type function is not JSON serializable"
    )


def test__invoke_task__overriding_the_serializer():
    task = Task(
        lambda: {"a": 2},
        outputs=dict(x=FromReturnValue(serializer=AsJSON(indent=1))),
    )
    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_task(task, params={}, output_path=tmp)

        with open(outputs["x"].filename, "rb") as f:
            assert f.read() == b'{\n "a": 2\n}'


def test__invoke_task__with_superfluous_parameters():
    task = Task(lambda: 1)

    with warnings.catch_warnings(record=True) as w:
        with tempfile.TemporaryDirectory() as tmp:
            invoke_task(task, params={"a": 1, "b": 2}, output_path=tmp)

        assert len(w) == 1
        assert (
            str(w[0].message)
            == "The following parameters were supplied to the task, but are not necessary: ['a', 'b']"
        )


def test__invoke_task__with_partitioned_output_from_iterator():
    class CustomIterator:
        def __init__(self):
            self.n = 2

        def __iter__(self):
            return self

        def __next__(self):
            if self.n <= 0:
                raise StopIteration

            self.n -= 1
            return self.n

    class CustomIterable:
        def __iter__(self):
            return CustomIterator()

    task = Task(
        lambda: {"partitioned": CustomIterable(), "not_partitioned": 1},
        outputs={
            "partitioned": FromKey("partitioned", is_partitioned=True),
            "not_partitioned": FromKey("not_partitioned"),
        },
    )

    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_task(task, params={}, output_path=tmp)
        assert deserialized_outputs(outputs) == {
            "not_partitioned": 1,
            "partitioned": [1, 0],
        }


def test__invoke_task__with_partitioned_output_that_cannot_be_partitioned():
    task = Task(
        lambda: 1,
        outputs={
            "not_partitioned": FromReturnValue(is_partitioned=True),
        },
    )
    with pytest.raises(TypeError) as e:
        with tempfile.TemporaryDirectory() as tmp:
            invoke_task(task, params={}, output_path=tmp)

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: Output 'not_partitioned' was declared as a partitioned output, but the return value was not an iterable (instead, it was of type 'int'). Partitioned outputs should be iterables of values (e.g. lists or sets). Each value in the iterable must be serializable with the serializer defined in the output."
    )
