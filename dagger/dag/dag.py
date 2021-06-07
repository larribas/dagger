"""Define the data structure for a DAG and validate all its components upon initialization."""
import re
from typing import Any, List, Mapping, NamedTuple, Set, Union
from typing import get_args as get_type_args

from dagger.dag.topological_sort import topological_sort
from dagger.data_structures import FrozenMapping
from dagger.input import FromNodeOutput, FromParam
from dagger.input import validate_name as validate_input_name
from dagger.output import validate_name as validate_output_name
from dagger.task import SupportedInputs as SupportedTaskInputs
from dagger.task import Task

VALID_NAME_REGEX = r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,63}$"
VALID_NAME = re.compile(VALID_NAME_REGEX)

SupportedInputs = Union[
    FromParam,
    FromNodeOutput,
]

Node = Union[
    Task,
    "DAG",
]


class DAGOutput(NamedTuple):
    """Define the output of a specific node as the output of the DAG."""

    node: str
    output: str


class DAG:
    """
    Data Structure that represents a DAG for later execution.

    DAGs are made up of:
    - A set of nodes, connected to each other through their inputs/outputs
    - A set of inputs to the DAG
    - A set of outputs from the DAG
    """

    def __init__(
        self,
        nodes: Mapping[str, Node],
        inputs: Mapping[str, SupportedInputs] = None,
        outputs: Mapping[str, DAGOutput] = None,
        runtime_options: Mapping[str, Any] = None,
    ):
        """
        Validate and initialize a DAG.

        Parameters
        ----------
        nodes
            A mapping from node names to nodes.
            Only certain types are allowed as nodes.

        inputs
            A mapping from input names to DAG inputs.
            Only certain types are allowed as inputs.

        outputs
            A mapping from output names to DAG outputs.
            Outputs must come from the output of a node within the DAG.

        runtime_options
            A list of options to supply to all runtimes.
            This allows you to take full advantage of the features of each runtime. For instance, you can use it to manipulate node affinities and tolerations in Kubernetes.
            Check the documentation of each runtime to see potential options.


        Returns
        -------
        A valid, immutable representation of a DAG


        Raises
        ------
        TypeError
            If any of the supplied parameters has an invalid type. Types should match the ones defined in the method signature.

        ValueError
            If any of the names are invalid, or any of the inputs/outputs point to non-existing nodes.

        CyclicDependencyError
            If the nodes contain cyclic dependencies to one another.
        """
        inputs = FrozenMapping(
            inputs or {},
            error_message="You may not mutate the inputs of a DAG after it has been initialized. We do this to guarantee the structures you build with dagger remain valid and consistent.",
        )
        outputs = FrozenMapping(
            outputs or {},
            error_message="You may not mutate the outputs of a DAG after it has been initialized. We do this to guarantee the structures you build with dagger remain valid and consistent.",
        )
        nodes = FrozenMapping(
            nodes,
            error_message="You may not mutate the nodes of a DAG after it has been initialized. We do this to guarantee the structures you build with dagger remain valid and consistent.",
        )

        _validate_nodes_are_not_empty(nodes)

        for node_name in nodes:
            _validate_node_name(node_name)

        for input_name in inputs:
            validate_input_name(input_name)
            _validate_input_is_supported(input_name, inputs[input_name])

        for output_name in outputs:
            validate_output_name(output_name)

        _validate_node_input_dependencies(nodes, inputs)
        _validate_outputs(nodes, outputs)

        self._nodes = nodes
        self._inputs = inputs
        self._outputs = outputs
        self._runtime_options = runtime_options or {}
        self._node_execution_order = topological_sort(
            {
                node_name: _node_dependencies(nodes[node_name].inputs)
                for node_name in nodes
            }
        )

    @property
    def nodes(self) -> Mapping[str, Node]:
        """Get the nodes that compose the DAG."""
        return self._nodes

    @property
    def inputs(self) -> Mapping[str, SupportedInputs]:
        """Get the inputs the DAG expects."""
        return self._inputs

    @property
    def outputs(self) -> Mapping[str, DAGOutput]:
        """Get the outputs the DAG produces."""
        return self._outputs

    @property
    def runtime_options(self) -> Mapping[str, Any]:
        """Get the specified runtime options."""
        return self._runtime_options

    @property
    def node_execution_order(self) -> List[Set[str]]:
        """
        Get a list of nodes to execute in order, respecting dependencies between nodes.

        Returns
        -------
        A list of sets
            Each set contains the names of the nodes that can be executed concurrently.
            The list represents the right order of execution for each of the sets.
        """
        return self._node_execution_order


def validate_parameters(
    inputs: Mapping[str, SupportedInputs],
    params: Mapping[str, bytes],
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
    # TODO: Use set differences to provide a more complete error message
    # TODO: Use warnings to warn about excessive/unused parameters
    for input_name in inputs:
        if input_name not in params:
            raise ValueError(
                f"The parameters supplied to this DAG were supposed to contain a parameter named '{input_name}', but only the following parameters were actually supplied: {list(params)}"
            )


def _validate_node_name(name: str):
    if not VALID_NAME.match(name):
        raise ValueError(
            f"'{name}' is not a valid name for a node. Node names must comply with the regex {VALID_NAME_REGEX}"
        )


def _node_dependencies(node_inputs: Mapping[str, SupportedTaskInputs]) -> Set[str]:
    return {
        input_from_node_output.node
        for input_from_node_output in node_inputs.values()
        if isinstance(input_from_node_output, FromNodeOutput)
    }


def _validate_outputs(
    dag_nodes: Mapping[str, Node],
    dag_outputs: Mapping[str, DAGOutput],
):
    for output_name in dag_outputs:
        output = dag_outputs[output_name]
        if output.node not in dag_nodes:
            raise ValueError(
                f"Output '{output_name}' depends on the output of a node named '{output.node}'. However, the DAG does not contain any node with such a name. These are the nodes contained by the DAG: {list(dag_nodes)}"
            )

        referenced_node_outputs = dag_nodes[output.node].outputs
        if output.output not in referenced_node_outputs:
            raise ValueError(
                f"Output '{output_name}' depends on the output '{output.output}' of another node named '{output.node}'. However, node '{output.node}' does not declare any output with such a name. These are the outputs defined by the node: {list(referenced_node_outputs)}"
            )


def _validate_nodes_are_not_empty(nodes: Mapping[str, Node]):
    if len(nodes) == 0:
        raise ValueError("A DAG needs to contain at least one node")


def _validate_node_input_dependencies(
    dag_nodes: Mapping[str, Node],
    dag_inputs: Mapping[str, SupportedInputs],
):
    for node_name in dag_nodes:
        node = dag_nodes[node_name]
        for input_name in node.inputs:
            dag_input = node.inputs[input_name]
            try:
                if isinstance(dag_input, FromParam):
                    _validate_input_from_param(input_name, dag_inputs)
                elif isinstance(dag_input, FromNodeOutput):
                    _validate_input_from_node_output(
                        node_name,
                        dag_input,
                        dag_nodes,
                    )
                else:
                    raise Exception(
                        f"Whoops. The current version of the library doesn't seem to support inputs of type '{type(dag_input)}'. This is most likely unintended. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
                    )

            except (TypeError, ValueError) as e:
                raise e.__class__(
                    f"Error validating input '{input_name}' of node '{node_name}': {str(e)}"
                )


def _validate_input_from_param(
    input_name: str,
    dag_inputs: Mapping[str, SupportedInputs],
):
    if input_name not in dag_inputs:
        raise ValueError(
            f"This input depends on a parameter named '{input_name}' being injected into the DAG. However, the DAG does not have any parameter with such a name. These are the parameters the DAG receives: {list(dag_inputs)}"
        )


def _validate_input_from_node_output(
    node_name: str,
    input: FromNodeOutput,
    dag_nodes: Mapping[str, Node],
):
    if input.node not in dag_nodes:
        raise ValueError(
            f"This input depends on the output of another node named '{input.node}'. However, the DAG does not define any node with such a name. These are the nodes contained by the DAG: {list(dag_nodes)}"
        )

    referenced_node_outputs = dag_nodes[input.node].outputs
    print(55, referenced_node_outputs)
    if input.output not in referenced_node_outputs:
        raise ValueError(
            f"This input depends on the output '{input.output}' of another node named '{input.node}'. However, node '{input.node}' does not declare any output with such a name. These are the outputs defined by the node: {list(referenced_node_outputs)}"
        )


def _validate_input_is_supported(input_name: str, input):
    if not _is_type_supported(input, SupportedInputs):
        raise TypeError(
            f"Input '{input_name}' is of type '{type(input).__name__}'. However, DAGs only support the following types of inputs: {[t.__name__ for t in get_type_args(SupportedInputs)]}"
        )


def _is_type_supported(obj, union):
    return any(
        [isinstance(obj, supported_type) for supported_type in get_type_args(union)]
    )
