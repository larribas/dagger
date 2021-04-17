import pytest

import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
from argo_workflows_sdk.node import Node, validate_name

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


def test__init__with_an_invalid_output_name():
    with pytest.raises(ValueError):
        Node(
            lambda: 1,
            outputs={
                "invalid name": outputs.FromKey("name"),
            },
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
    ]

    for name in valid_names:
        # We are testing it doesn't raise any validation errors
        validate_name(name)


def test__validate_name__with_invalid_names():
    invalid_names = [
        "",
        "name with spaces",
        "too long" * 30,
        "with$ymßols",
        "with_underscores",
    ]

    for name in invalid_names:
        with pytest.raises(ValueError) as e:
            validate_name(name)

        assert (
            str(e.value)
            == f"'{name}' is not a valid name for a node. Node names must comply with the regex ^[a-zA-Z0-9][a-zA-Z0-9-]{{0,128}}$"
        )
