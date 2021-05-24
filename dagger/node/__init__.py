import re
from typing import Callable, Dict, List, Union
from typing import get_args as get_type_args

from dagger.inputs import FromNodeOutput, FromParam
from dagger.inputs import validate_name as validate_input_name
from dagger.outputs import FromKey, FromProperty, FromReturnValue
from dagger.outputs import validate_name as validate_output_name

VALID_NAME_REGEX = r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$"
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

        for input_name, input in inputs.items():
            validate_input_name(input_name)
            validate_input_is_supported(input_name, input)

        for output_name, output in outputs.items():
            validate_output_name(output_name)
            validate_output_is_supported(output_name, output)

        validate_callable_inputs_match_defined_inputs(func, list(inputs.keys()))

        self.inputs = inputs
        self.outputs = outputs
        self.func = func


def validate_name(name: str):
    if not VALID_NAME.match(name):
        raise ValueError(
            f"'{name}' is not a valid name for a node. Node names must comply with the regex {VALID_NAME_REGEX}"
        )


def validate_input_is_supported(input_name, input):
    if not is_type_supported(input, SupportedInputs):
        raise ValueError(
            f"Input '{input_name}' is of type '{type(input).__name__}'. However, nodes only support the following types of inputs: {[t.__name__ for t in get_type_args(SupportedInputs)]}"
        )


def validate_output_is_supported(output_name, output):
    if not is_type_supported(output, SupportedOutputs):
        raise ValueError(
            f"Output '{output_name}' is of type '{type(output).__name__}'. However, nodes only support the following types of outputs: {[t.__name__ for t in get_type_args(SupportedOutputs)]}"
        )


def is_type_supported(obj, union: Union):
    return any(
        [isinstance(obj, supported_type) for supported_type in get_type_args(union)]
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
