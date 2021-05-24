import os
import tempfile

import pytest

import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
from argo_workflows_sdk import Node
from argo_workflows_sdk.runtime.cli.node import invoke

#
# Invoke
#


def test__invoke__without_inputs_or_outputs():
    invocations = []
    node = Node(lambda: invocations.append(1))
    invoke(node)
    assert invocations == [1]


def test__invoke__with_missing_input_parameter():
    node = Node(
        lambda a: 1,
        inputs=dict(a=inputs.FromParam()),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.NamedTemporaryFile() as f:
            invoke(node, input_locations=dict(x=f.name))

    assert (
        str(e.value)
        == "This node is supposed to receive a pointer to an input named 'a'. However, only the following input pointers were supplied: ['x']"
    )


def test__invoke__with_missing_output_parameter():
    node = Node(
        lambda: 1,
        outputs=dict(a=outputs.FromReturnValue()),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.NamedTemporaryFile() as f:
            invoke(node, output_locations=dict(x=f.name))

    assert (
        str(e.value)
        == "This node is supposed to receive a pointer to an output named 'a'. However, only the following output pointers were supplied: ['x']"
    )


def test__invoke__with_multiple_inputs_and_outputs():
    node = Node(
        lambda first_name, last_name: dict(
            message=f"Hello {first_name} {last_name}",
            name_length=len(first_name) + len(last_name),
        ),
        inputs=dict(
            first_name=inputs.FromParam(),
            last_name=inputs.FromParam(),
        ),
        outputs=dict(
            message=outputs.FromKey("message"),
            name_length=outputs.FromKey("name_length"),
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

        invoke(
            node,
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
