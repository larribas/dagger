import pytest

from dagger.input.from_node_output import FromNodeOutput
from dagger.input.from_param import FromParam
from dagger.input.validators import (
    _clean_parameters,
    _validate_parameters,
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


def test__validate_and_clean_parameters__when_we_provide_required_and_superfluous_params_but_no_optional_params():
    clean_params = validate_and_clean_parameters(
        inputs={
            "a": FromParam(),
            "b": FromNodeOutput("n", "o"),
            "c": FromParam(default_value=10),
        },
        params={
            "a": 1,
            "b": "2",
            "d": 3,
        },
    )
    assert clean_params == {"a": 1, "b": "2", "c": 10}


def test__validate_and_clean_parameters__when_a_required_input_is_missing():
    with pytest.raises(ValueError) as e:
        validate_and_clean_parameters(
            inputs={
                "a": FromParam(),
                "b": FromNodeOutput("n", "o"),
                "c": FromParam(default_value=10),
            },
            params={
                "a": 1,
                "c": 1,
            },
        )

    assert (
        str(e.value)
        == "The parameters supplied to this node were supposed to contain the "
        "following parameters: ['a', 'b']. However, only the following "
        "parameters were actually supplied: ['a', 'c']. We are missing: ['b']."
    )


#
# _validate_parameters
#


def test__validate_parameters__when_there_are_no_required_inputs():
    _validate_parameters(
        required_inputs={},
        params={},
    )
    # we are asserting that no validation errors are raised


def test__validate_parameters__when_all_required_inputs_are_passed():
    _validate_parameters(
        required_inputs={
            "a": FromParam(),
            "b": FromNodeOutput("n", "o"),
        },
        params={
            "a": 1,
            "b": "2",
        },
    )
    # we are asserting that no validation errors are raised


def test__validate_parameters__when_we_are_passing_superfluous_params():
    _validate_parameters(
        required_inputs={
            "a": FromParam(),
        },
        params={
            "a": 1,
            "b": 2,
        },
    )
    # we are asserting that no validation errors are raised


def test__validate_parameters__when_a_required_input_is_missing():
    with pytest.raises(ValueError) as e:
        _validate_parameters(
            required_inputs={
                "a": FromParam(),
                "b": FromNodeOutput("n", "o"),
                "c": FromParam(),
            },
            params={
                "a": 1,
                "c": 1,
            },
        )

    assert (
        str(e.value)
        == "The parameters supplied to this node were supposed to contain the "
        "following parameters: ['a', 'b', 'c']. However, only the following "
        "parameters were actually supplied: ['a', 'c']. We are missing: ['b']."
    )


#
# _clean_parameters
#


def test__clean_parameters__removes_superfluous_parameters():
    clean_params = _clean_parameters(
        required_inputs={
            "a": FromParam(),
        },
        optional_inputs={},
        params={
            "a": 1,
            "b": 1,
        },
    )
    assert clean_params == {"a": 1}


def test__clean_parameters__includes_default_values_that_were_not_passed_as_parameters():
    clean_params = _clean_parameters(
        required_inputs={
            "a": FromParam(),
        },
        optional_inputs={
            "b": FromParam(default_value=10),
            "c": FromParam(default_value=20),
        },
        params={
            "a": 1,
            "c": 2,
        },
    )
    assert clean_params == {"a": 1, "b": 10, "c": 2}


def test__clean_parameters__overriding_a_default_with_falsey_value():
    falsey_values = [
        None,
        [],
        {},
        False,
    ]

    for falsey_value in falsey_values:
        clean_params = _clean_parameters(
            required_inputs={},
            optional_inputs={
                "a": FromParam(default_value=10),
            },
            params={
                "a": falsey_value,
            },
        )
        assert clean_params == {"a": falsey_value}
