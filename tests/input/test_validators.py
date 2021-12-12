import pytest

from dagger.input import (
    FromNodeOutput,
    FromParam,
    split_required_and_optional_inputs,
    validate_and_clean_parameters,
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


#
# validate_and_clean_parameters
#


def test__validate_and_clean_parameters__when_params_match_inputs():
    clean_params = validate_and_clean_parameters(
        inputs={
            "a": FromParam(),
            "b": FromNodeOutput("n", "o"),
        },
        params={
            "a": 1,
            "b": "2",
        },
    )
    assert clean_params == {"a": 1, "b": "2"}


def test__validate_and_clean_parameters__when_a_required_input_is_missing():
    with pytest.raises(ValueError) as e:
        validate_and_clean_parameters(
            inputs={
                "c": FromParam(),
                "a": FromParam(),
                "b": FromNodeOutput("n", "o"),
            },
            params={
                "c": 1,
                "a": 1,
            },
        )

    assert (
        str(e.value)
        == "The parameters supplied to this node were supposed to contain the "
        "following parameters: ['a', 'b', 'c']. However, only the following "
        "parameters were actually supplied: ['a', 'c']. We are missing: ['b']."
    )


def test__validate_and_clean_parameters__removes_superfluous_parameters():
    clean_params = validate_and_clean_parameters(
        inputs={
            "c": FromParam(),
            "a": FromParam(),
        },
        params={
            "z": 1,
            "a": 1,
            "c": 1,
        },
    )
    assert clean_params == {"a": 1, "c": 1}


def test__validate_and_clean_parameters__includes_default_values_that_were_not_passed_as_parameters():
    clean_params = validate_and_clean_parameters(
        inputs={
            "a": FromParam(),
            "b": FromParam(default_value=10),
            "c": FromParam(default_value=20),
        },
        params={
            "a": 1,
            "c": 2,
        },
    )
    assert clean_params == {"a": 1, "b": 10, "c": 2}


def test__validate_and_clean_parameters__overriding_a_default_with_falsey_value():
    falsey_values = [
        None,
        [],
        {},
        False,
    ]

    for falsey_value in falsey_values:
        clean_params = validate_and_clean_parameters(
            inputs={
                "a": FromParam(default_value=10),
            },
            params={
                "a": falsey_value,
            },
        )
        assert clean_params == {"a": falsey_value}
