"""Define tasks in a workflow/pipeline."""
from typing import Callable, List, Mapping, Union
from typing import get_args as get_type_args

from dagger.data_structures import FrozenMapping
from dagger.input import FromNodeOutput, FromParam
from dagger.input import validate_name as validate_input_name
from dagger.output import FromKey, FromProperty, FromReturnValue
from dagger.output import validate_name as validate_output_name

SupportedInputs = Union[
    FromParam,
    FromNodeOutput,
]

SupportedOutputs = Union[
    FromReturnValue,
    FromKey,
    FromProperty,
]


class Task:
    """A task that executes a given function taking inputs from the specified sources and producing the specified outputs."""

    def __init__(
        self,
        func: Callable,
        inputs: Mapping[str, SupportedInputs] = None,
        outputs: Mapping[str, SupportedOutputs] = None,
    ):
        """
        Validate and initialize a Task.

        Parameters
        ----------
        func
            The Python function for the Task to execute

        inputs
            A mapping from input names to Task inputs.
            Only certain types are allowed as inputs.

        outputs
            A mapping from output names to Task outputs.
            Outputs must be retrievable from the function's outputs.


        Returns
        -------
        A valid, immutable representation of a Task


        Raises
        ------
        TypeError
            If any of the inputs/outputs is not supported.
            If inputs do not match the arguments of the function.

        ValueError
            If the names of the inputs/outputs have unsupported characters.
        """
        inputs = FrozenMapping(
            inputs or {},
            error_message="You may not mutate the inputs of a task. We do this to guarantee that, once initialized, the structures you build with dagger remain valid and consistent.",
        )
        outputs = FrozenMapping(
            outputs or {},
            error_message="You may not mutate the outputs of a task. We do this to guarantee that, once initialized, the structures you build with dagger remain valid and consistent.",
        )

        for input_name in inputs:
            validate_input_name(input_name)
            _validate_input_is_supported(input_name, inputs[input_name])

        for output_name in outputs:
            validate_output_name(output_name)
            _validate_output_is_supported(output_name, outputs[output_name])

        _validate_callable_inputs_match_defined_inputs(func, list(inputs))

        self._inputs = inputs
        self._outputs = outputs
        self._func = func

    @property
    def func(self) -> Callable:
        """Get the function associated with the Task."""
        return self._func

    @property
    def inputs(self) -> Mapping[str, SupportedInputs]:
        """Get the inputs the Task expects."""
        return self._inputs

    @property
    def outputs(self) -> Mapping[str, SupportedOutputs]:
        """Get the outputs the Task produces."""
        return self._outputs


def _validate_input_is_supported(input_name, input):
    if not _is_type_supported(input, SupportedInputs):
        raise TypeError(
            f"Input '{input_name}' is of type '{type(input).__name__}'. However, nodes only support the following types of inputs: {[t.__name__ for t in get_type_args(SupportedInputs)]}"
        )


def _validate_output_is_supported(output_name, output):
    if not _is_type_supported(output, SupportedOutputs):
        raise TypeError(
            f"Output '{output_name}' is of type '{type(output).__name__}'. However, nodes only support the following types of outputs: {[t.__name__ for t in get_type_args(SupportedOutputs)]}"
        )


def _is_type_supported(obj, union: Union):
    return any(
        [isinstance(obj, supported_type) for supported_type in get_type_args(union)]
    )


def _validate_callable_inputs_match_defined_inputs(
    func: Callable,
    input_names: List[str],
):
    from inspect import signature

    sig = signature(func)

    try:
        sig.bind(**{name: None for name in input_names})
    except TypeError as e:
        raise TypeError(
            f"This node was declared with the following inputs: {input_names}. However, the node's function has the following signature: {str(sig)}. The inputs could not be bound to the parameters because: {e.args[0]}"
        )
