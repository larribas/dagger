"""Validators applicable to all types of inputs."""
import re
import warnings
from typing import Any, Mapping, Union

from dagger.input import FromNodeOutput, FromParam
from dagger.input.empty_default_value import EmptyDefaultValue

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


def validate_parameters(
    inputs: Mapping[str, Union[FromParam, FromNodeOutput]],
    params: Mapping[str, Any],
):
    """
    Validate a series of parameters against the inputs of a DAG.

    Parameters
    ----------
    inputs
        A mapping of input names to inputs.

    params
        A mapping of input names to parameters or input values.
        Input values must be passed in their serialized representation.

    Raises
    ------
    ValueError
        If the set of parameters does not contain all the required inputs.
    """
    required_inputs = {
        name
        for name, input_ in inputs.items()
        if not isinstance(input_, FromParam)
        or input_.default_value == EmptyDefaultValue()
    }
    missing_params = required_inputs - params.keys()
    if missing_params:
        raise ValueError(
            f"The parameters supplied to this node were supposed to contain the "
            f"following parameters: {sorted(list(required_inputs))}. However, only the "
            f"following parameters were actually supplied: {sorted(list(params))}. We "
            f"are missing: {sorted(list(missing_params))}."
        )

    superfluous_params = params.keys() - inputs.keys()
    if superfluous_params:
        warnings.warn(
            f"The following parameters were supplied to this node, but are not "
            f"necessary: {sorted(list(superfluous_params))}"
        )
