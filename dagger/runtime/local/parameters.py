"""Functions to handle node invocation parameters."""

from typing import Any, Mapping, Union

from dagger.input import FromNodeOutput, FromParam, split_required_and_optional_inputs


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
