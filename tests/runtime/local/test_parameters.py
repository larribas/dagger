import pytest

from dagger.input import FromNodeOutput, FromParam
from dagger.runtime.local.parameters import validate_and_clean_parameters


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
