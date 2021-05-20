import re
from typing import Callable, Dict, List, Union

from argo_workflows_sdk.inputs import FromNodeOutput, FromParam
from argo_workflows_sdk.inputs import validate_name as validate_input_name
from argo_workflows_sdk.outputs import FromKey, FromProperty, FromReturnValue
from argo_workflows_sdk.outputs import validate_name as validate_output_name

VALID_NAME_REGEX = r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,128}$"
VALID_NAME = re.compile(VALID_NAME_REGEX)


SupportedInputs = Union[
    FromParam,
    FromNodeOutput,
]

SupportedOutputs = Union[
    FromReturnValue,
    FromKey,
    FromProperty,
]


class Node:
    def __init__(
        self,
        func: Callable,
        inputs: Dict[str, SupportedInputs] = None,
        outputs: Dict[str, SupportedOutputs] = None,
    ):
        inputs = inputs or {}
        outputs = outputs or {}

        for input_name in inputs:
            validate_input_name(input_name)
            # TODO: Validate the inputs are part of the set of supported inputs

        for output_name in outputs:
            validate_output_name(output_name)
            # TODO: Validate the outputs are part of the set of supported outputs

        validate_callable_inputs_match_defined_inputs(func, list(inputs.keys()))

        self.inputs = inputs
        self.outputs = outputs
        self.func = func


def validate_name(name: str):
    if not VALID_NAME.match(name):
        raise ValueError(
            f"'{name}' is not a valid name for a node. Node names must comply with the regex {VALID_NAME_REGEX}"
        )


def validate_callable_inputs_match_defined_inputs(
    func: Callable, input_names: List[str]
):
    from inspect import signature

    sig = signature(func)

    try:
        sig.bind(**{name: None for name in input_names})
    except TypeError as e:
        raise TypeError(
            f"This node was declared with the following inputs: {input_names}. However, the node's function has the following signature: {str(sig)}. The inputs could not be bound to the parameters because: {e.args[0]}"
        )
