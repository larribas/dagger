import pytest

from dagger.input import (
    FromNodeOutput,
    FromParam,
    split_required_and_optional_inputs,
    validate_name,
)

#
# validate_name
#


def test__validate_name__with_valid_names():
    valid_names = [
        "param",
        "name-with-dashes",
        "name_with_underscores",
        "name-with-dashes_and_underscores_and_123",
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
    ]

    for name in invalid_names:
        with pytest.raises(ValueError) as e:
            validate_name(name)

        assert (
            str(e.value)
            == f"'{name}' is not a valid name for an input. Inputs must comply with the regex ^[a-zA-Z0-9][a-zA-Z0-9-_]{{0,63}}$"
        )


#
# split_required_and_optional_inputs
#


def test__split_required_and_optional_inputs():
    required, optional = split_required_and_optional_inputs(
        {
            "req1": FromParam(),
            "req2": FromNodeOutput("x", "y"),
            "opt1": FromParam(default_value=2),
            "opt2": FromParam(default_value=None),
        }
    )

    assert required == {
        "req1": FromParam(),
        "req2": FromNodeOutput("x", "y"),
    }
    assert optional == {
        "opt1": FromParam(default_value=2),
        "opt2": FromParam(default_value=None),
    }
