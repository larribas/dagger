"""Command-line Interface to run DAGs or Tasks taking their inputs from files and storing their outputs into files."""
from typing import Any, Iterable, List, Mapping

import dagger.runtime.local as local
from dagger.dag import DAG
from dagger.runtime.cli.locations import (
    retrieve_input_from_location,
    store_output_in_location,
)
from dagger.runtime.cli.nested_nodes import NodeWithParent, find_nested_node


def invoke_with_locations(
    dag: DAG,
    node_address: List[str] = None,
    input_locations: Mapping[str, str] = None,
    output_locations: Mapping[str, str] = None,
):
    """
    Invoke the supplied DAG (or a node therein) retrieving the inputs from, and storing the outputs into, the specified locations.

    Parameters
    ----------
    dag : DAG
        DAG to execute

    node_address
        The address of the nested node. Each item in the list represents a level of nesting.
        Empty references the `dag` parameter.

    input_locations
        A mapping of input names to input locations

    output_locations
        A mapping of output names to output locations


    Raises
    ------
    ValueError
        When the location of any required input/output is missing

    TypeError
        When any of the outputs cannot be obtained from the return value of their node

    SerializationError
        When some of the outputs cannot be serialized with the specified Serializer
    """
    input_locations = input_locations or {}
    output_locations = output_locations or {}
    nested_node = find_nested_node(dag, node_address or [])

    _validate_inputs(nested_node.node.inputs.keys(), input_locations.keys())
    _validate_outputs(nested_node.node.outputs.keys(), output_locations.keys())

    params = _deserialized_params(nested_node, input_locations)

    outputs = local.invoke(nested_node.node, params)

    for output_name in output_locations:
        store_output_in_location(
            output_location=output_locations[output_name],
            output_value=outputs[output_name],
        )


def _validate_inputs(
    input_names: Iterable[str],
    input_locations: Iterable[str],
):
    """Validate all the input locations supplied against the inputs the node is expecting."""
    for input_name in input_names:
        if input_name not in input_locations:
            raise ValueError(
                f"This node is supposed to receive a pointer to an input named '{input_name}'. However, only the following input pointers were supplied: {sorted(input_locations)}"
            )


def _validate_outputs(
    output_names: Iterable[str],
    output_locations: Iterable[str],
):
    """Validate all the output locations supplied against the outputs the node is generating."""
    for output_name in output_names:
        if output_name not in output_locations:
            raise ValueError(
                f"This node is supposed to receive a pointer to an output named '{output_name}'. However, only the following output pointers were supplied: {sorted(output_locations)}"
            )


def _deserialized_params(
    nested_node: NodeWithParent,
    input_locations: Mapping[str, str],
) -> Mapping[str, Any]:
    """Retrieve and deserialize all the parameters expected by a Node."""
    params = {}
    for input_name in input_locations:
        input_value = retrieve_input_from_location(input_locations[input_name])
        input_type = nested_node.node.inputs[input_name]

        if isinstance(input_value, local.PartitionedOutput):
            params[input_name] = [
                input_type.serializer.deserialize(partition)
                for partition in input_value
            ]
        else:
            params[input_name] = input_type.serializer.deserialize(input_value)

    return params
