"""Validators applicable to all types of inputs."""
import re
from typing import Any, Mapping, Tuple, Union

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


def validate_and_clean_parameters(
    inputs: Mapping[str, Union[FromParam, FromNodeOutput]], params: Mapping[str, Any]
) -> Mapping[str, Any]:
    """
    Build an exhaustive map of parameters for a node, removing superfluous parameters and adding any default values that haven't been overridden.

    Parameters
    ----------
    inputs
        A mapping of input names to inputs.

    params
        A mapping of input names to their values.

    Raises
    ------
    ValueError
        If any required inputs are missing.

    Returns
    -------
    A mapping of input name to input value, with no
    """
    required_inputs, optional_inputs = split_required_and_optional_inputs(inputs)

    missing_params = required_inputs.keys() - params.keys()
    if missing_params:
        raise ValueError(
            f"The parameters supplied to this node were supposed to contain the "
            f"following parameters: {sorted(list(required_inputs))}. However, only the "
            f"following parameters were actually supplied: {sorted(list(params))}. We "
            f"are missing: {sorted(list(missing_params))}."
        )

    required_params = {input_name: params[input_name] for input_name in required_inputs}
    optional_params = {
        input_name: params[input_name]
        if input_name in params
        else input_type.default_value
        for input_name, input_type in optional_inputs.items()
    }

    return {**required_params, **optional_params}
