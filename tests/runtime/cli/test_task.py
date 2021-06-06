import os
import tempfile

import pytest

import dagger.input as input
import dagger.output as output
from dagger.runtime.cli.task import invoke_task
from dagger.task import Task


def test__invoke_task__without_inputs_or_outputs():
    invocations = []
    task = Task(lambda: invocations.append(1))
    invoke_task(task)
    assert invocations == [1]


def test__invoke_task__with_missing_input_parameter():
    task = Task(
        lambda a: 1,
        inputs=dict(a=input.FromParam()),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.NamedTemporaryFile() as f:
            invoke_task(task, input_locations=dict(x=f.name))

    assert (
        str(e.value)
        == "This task is supposed to receive a pointer to an input named 'a'. However, only the following input pointers were supplied: ['x']"
    )


def test__invoke_task__with_missing_output_parameter():
    task = Task(
        lambda: 1,
        outputs=dict(a=output.FromReturnValue()),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.NamedTemporaryFile() as f:
            invoke_task(task, output_locations=dict(x=f.name))

    assert (
        str(e.value)
        == "This task is supposed to receive a pointer to an output named 'a'. However, only the following output pointers were supplied: ['x']"
    )


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

    with tempfile.TemporaryDirectory() as tmp:
        first_name_input = os.path.join(tmp, "first_name")
        last_name_input = os.path.join(tmp, "last_name")
        message_output = os.path.join(tmp, "message")
        name_length_output = os.path.join(tmp, "name_length")

        with open(first_name_input, "wb") as f:
            f.write(b'"John"')

        with open(last_name_input, "wb") as f:
            f.write(b'"Doe"')

        invoke_task(
            task,
            input_locations=dict(
                first_name=first_name_input,
                last_name=last_name_input,
            ),
            output_locations=dict(
                message=message_output,
                name_length=name_length_output,
            ),
        )

        with open(message_output, "rb") as f:
            assert f.read() == b'"Hello John Doe"'

        with open(name_length_output, "rb") as f:
            assert f.read() == b"7"
