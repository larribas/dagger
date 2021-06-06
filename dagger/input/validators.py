"""Validators applicable to all types of inputs."""
import re

VALID_NAME_REGEX = r"^[a-zA-Z0-9][a-zA-Z0-9-_]{0,63}$"
VALID_NAME = re.compile(VALID_NAME_REGEX)


def validate_name(name: str):
    """
    Verify whether a string is a valid name for an input.

    Raises
    ------
    ValueError
        If the name doesn't match the constraints set for an input name.
    """
    if not VALID_NAME.match(name):
        raise ValueError(
            f"'{name}' is not a valid name for an input. Inputs must comply with the regex {VALID_NAME_REGEX}"
        )
