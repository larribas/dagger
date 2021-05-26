"""Define DAGs of Nodes."""
import re
from typing import Dict

from dagger.dag.dag import DAG, DAGOutput, SupportedInputs, SupportedNodes
from dagger.dag.topological_sort import CyclicDependencyError

VALID_NAME_REGEX = r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$"
VALID_NAME = re.compile(VALID_NAME_REGEX)


def validate_name(name: str):
    """
    Validate the name of a DAG.

    Raises
    ------
    ValueError
        If the name is invalid according to the library's constraints.
    """
    if not VALID_NAME.match(name):
        raise ValueError(
            f"'{name}' is not a valid name for a DAG. DAG names must comply with the regex {VALID_NAME_REGEX}"
        )


def validate_parameters(
    inputs: Dict[str, SupportedInputs],
    params: Dict[str, bytes],
):
    """
    Validate a series of parameters against the inputs of a DAG.

    Parameters
    ----------
    inputs : Dictionary of str -> input
        A dictionary of inputs indexed by their names.

    params : Dictionary of str -> bytes
        A dictionary of parameters (in their serialized representation), indexed by their names.

    Raises
    ------
    ValueError
        If the set of parameters does not contain all the required inputs.
    """
    # TODO: Use set differences to provide a more complete error message
    # TODO: Use warnings to warn about excessive/unused parameters
    for input_name in inputs.keys():
        if input_name not in params:
            raise ValueError(
                f"The parameters supplied to this DAG were supposed to contain a parameter named '{input_name}', but only the following parameters were actually supplied: {list(params.keys())}"
            )


__all__ = [
    "DAG",
    "DAGOutput",
    "SupportedInputs",
    "SupportedNodes",
    # Exceptions
    "CyclicDependencyError",
    # Validation
    "validate_name",
    "validate_parameters",
]
