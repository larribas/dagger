import pytest

from argo_workflows_sdk.inputs import validate_name


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
