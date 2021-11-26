import warnings

import pytest

from dagger.input import FromNodeOutput, FromParam, validate_name, validate_parameters

#
# validate_parameters
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
# validate_parameters
#


def test__validate_parameters__when_params_match_inputs():
    validate_parameters(
        inputs={
            "a": FromParam(),
            "b": FromNodeOutput("n", "o"),
        },
        params={
            "a": 1,
            "b": "2",
        },
    )
    # We are testing there are no exceptions raised as a result of calling the validator


def test__validate_parameters__when_input_is_missing():
    with pytest.raises(ValueError) as e:
        validate_parameters(
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


def test__validate_parameters__when_param_is_superfluous():
    with warnings.catch_warnings(record=True) as w:
        validate_parameters(
            inputs={
                "c": FromParam(),
                "a": FromParam(),
            },
            params={
                "z": 1,
                "a": 1,
                "c": 1,
                "y": 1,
            },
        )
        assert len(w) == 1
        assert (
            str(w[0].message)
            == "The following parameters were supplied to this node, but are not "
            "necessary: ['y', 'z']"
        )


def test__validate_parameters__if_superfluous_params_warns():
    # given
    inputs = {"a": FromParam("a")}
    params = {"a": 1, "b": 2}
    # when
    with pytest.warns(Warning) as record:
        validate_parameters(inputs, params)
        if not record:
            pytest.fail("Expected a warning!")
    # then
    # check that only one warning was raised
    assert len(record) == 1
    # check that the message matches
    assert (
        record[0].message.args[0] == "The following parameters were supplied to this "
        "node, but are not necessary: ['b']"
    )


def test__validate_parameters__no_superfluous_params_does_not_warn():
    # given
    inputs = {"a": FromParam("a"), "b": FromParam("b", 2), "c": FromParam("c", 3)}
    params = {"a": 1, "b": 4}
    # when
    with pytest.warns(None) as record:
        validate_parameters(inputs, params)

    # check no warning was raised
    assert len(record) == 0
