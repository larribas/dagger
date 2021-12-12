"""Validators applicable to all types of inputs."""
import re
from typing import Mapping, Tuple, Union

from dagger.input import FromNodeOutput, FromParam

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
            f"'{name}' is not a valid name for an input. Inputs must comply with the "
            f"regex {VALID_NAME_REGEX}"
        )


def split_required_and_optional_inputs(
    inputs: Mapping[str, Union[FromParam, FromNodeOutput]],
) -> Tuple[Mapping[str, Union[FromParam, FromNodeOutput]], Mapping[str, FromParam],]:
    """
    Split a map of inputs into a tuple of (required, optional) input maps.

    Parameters
    ----------
    inputs
        A mapping of input names to inputs.

    params
        A mapping of input names to their values.


    Returns
    -------
    A 2-tuple of (required, optional) input mappings.
    """
    required, optional = {}, {}

    for input_name, input_type in inputs.items():
        if isinstance(input_type, FromParam) and input_type.has_default_value():
            optional[input_name] = input_type
        else:
            required[input_name] = input_type

    return required, optional
