import pytest

import dagger.input as input
import dagger.output as output
from dagger import Task
from dagger.runtime.local.task import invoke_task
from dagger.serializer import SerializationError


def test__invoke_task__without_inputs_or_outputs():
    invocations = []
    task = Task(lambda: invocations.append(1))
    assert invoke_task(task) == {}
    assert invocations == [1]


def test__invoke_task__with_single_input_and_output():
    task = Task(
        lambda number: number * 2,
        inputs=dict(number=input.FromParam()),
        outputs=dict(doubled_number=output.FromReturnValue()),
    )
    assert invoke_task(task, params=dict(number=b"2")) == dict(doubled_number=b"4")


def test__invoke_task__with_multiple_inputs_and_outputs():
    task = Task(
        lambda first_name, last_name: dict(
            message=f"Hello {first_name} {last_name}",
            name_length=len(first_name) + len(last_name),
        ),
        inputs=dict(
            first_name=input.FromParam(),
            last_name=input.FromParam(),
        ),
        outputs=dict(
            message=output.FromKey("message"),
            name_length=output.FromKey("name_length"),
        ),
    )
    assert invoke_task(
        task,
        params=dict(
            first_name=b'"John"',
            last_name=b'"Doe"',
        ),
    ) == dict(
        message=b'"Hello John Doe"',
        name_length=b"7",
    )


def test__invoke_task__with_missing_input_parameter():
    task = Task(lambda a: 1, inputs=dict(a=input.FromParam()))
    with pytest.raises(ValueError) as e:
        invoke_task(task, params={})

    assert (
        str(e.value)
        == "The parameters supplied to this task were supposed to contain a parameter named 'a', but only the following parameters were actually supplied: []"
    )


def test__invoke_task__with_mismatched_outputs():
    task = Task(lambda: 1, outputs=dict(a=output.FromKey("x")))
    with pytest.raises(TypeError) as e:
        invoke_task(task, params={})

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: This output is of type FromKey. This means we expect the return value of the function to be a mapping containing, at least, a key named 'x'"
    )


def test__invoke_task__with_missing_outputs():
    task = Task(lambda: dict(a=1), outputs=dict(x=output.FromKey("x")))
    with pytest.raises(ValueError) as e:
        invoke_task(task, params={})

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: An output of type FromKey('x') expects the return value of the function to be a mapping containing, at least, a key named 'x'"
    )


def test__invoke_task__with_unserializable_outputs():
    task = Task(lambda: dict(a=lambda: 2), outputs=dict(x=output.FromKey("a")))
    with pytest.raises(SerializationError) as e:
        invoke_task(task, params={})

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: Object of type function is not JSON serializable"
    )
