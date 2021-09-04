"""Define tasks in a workflow/pipeline."""
from typing import Any, Callable, List, Mapping, Optional, Union
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
        runtime_options: Mapping[str, Any] = None,
        partition_by_input: Optional[str] = None,
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

        runtime_options
            A list of options to supply to all runtimes.
            This allows you to take full advantage of the features of each runtime. For instance, you can use it to manipulate node affinities and tolerations in Kubernetes.
            Check the documentation of each runtime to see potential options.

        partition_by_input
            If specified, it signals the task should be run as many times as partitions in the specified input.
            Each of the executions will only receive one of the partitions of that input.


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
            If the partition_by field doesn't link to a valid input.
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

        if partition_by_input:
            _validate_partitioned_input(partition_by_input, inputs)
            _validate_there_are_no_partitioned_outputs(outputs)

        self._inputs = inputs
        self._outputs = outputs
        self._func = func
        self._runtime_options = runtime_options or {}
        self._partition_by_input = partition_by_input

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
        """Get the outputs the task produces."""
        return self._outputs

    @property
    def runtime_options(self) -> Mapping[str, Any]:
        """Get the specified runtime options."""
        return self._runtime_options

    @property
    def partition_by_input(self) -> Optional[str]:
        """Return the input this task should be partitioned by, if any."""
        return self._partition_by_input

    def __eq__(self, obj) -> bool:
        """Return true if the two tasks are equivalent to each other."""
        return (
            self._func == obj._func
            and self._inputs == obj._inputs
            and self._outputs == obj._outputs
            and self._runtime_options == obj._runtime_options
        )

    def __repr__(self) -> str:
        """Return a human-readable representation of the task."""
        return f"Task(func={self._func}, inputs={self._inputs}, outputs={self._outputs}, runtime_options={self._runtime_options}, partition_by_input={self._partition_by_input})"


def _validate_input_is_supported(input_name, input_type):
    if not _is_type_supported(input_type, SupportedInputs):
        raise TypeError(
            f"Input '{input_name}' is of type '{type(input_type).__name__}'. However, nodes only support the following types of inputs: {[t.__name__ for t in get_type_args(SupportedInputs)]}"
        )


def _validate_partitioned_input(
    partition_by_input: str,
    inputs: Mapping[str, SupportedInputs],
):
    if partition_by_input not in inputs:
        raise ValueError(
            f"This node is partitioned by '{partition_by_input}'. However, '{partition_by_input}' is not an input of the node. The available inputs are {sorted(list(inputs))}."
        )

    if isinstance(inputs[partition_by_input], FromParam):
        raise ValueError(
            "Nodes may not be partitioned by an input that comes from a parameter. This is not a valid map-reduce pattern in dagger. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
        )


def _validate_there_are_no_partitioned_outputs(outputs: Mapping[str, SupportedOutputs]):
    for output_name, output_type in outputs.items():
        if output_type.is_partitioned:
            raise ValueError(
                "Partitioned nodes may not generate partitioned outputs. This is not a valid map-reduce pattern in dagger. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
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
            f"This node was declared with the following inputs: {input_names}. However, the node's function has the following signature: {str(sig)}. The inputs could not be bound to the parameters because: {e.args[0]}."
        )
