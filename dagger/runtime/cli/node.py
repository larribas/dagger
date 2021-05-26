"""Run nodes taking their inputs and outputs from files."""
from typing import Dict

import dagger.runtime.local as local
from dagger.node import Node
from dagger.runtime.cli.locations import (
    retrieve_input_from_location,
    store_output_in_location,
)


def invoke_node(
    node: Node,
    input_locations: Dict[str, str] = None,
    output_locations: Dict[str, str] = None,
):
    """
    Invoke a node. Take the inputs from and store the outputs in the locations specified.

    Parameters
    ----------
    node : Node
        Node to execute

    input_locations : Dictionary of str -> str
        Location of the files that contain the values for the inputs of the node.
        The contents of the files must be serialized in a binary format.
        The dictionary keys are the names of the inputs.

    output_locations : Dictionary of str -> str
        Location of the files where the outputs of the node should be stored.
        The contents of the files will be serialized in a binary format.
        The dictionary keys are the names of the outputs.


    Raises
    ------
    ValueError
        When the location of any required input/output is missing

    TypeError
        When any of the outputs cannot be obtained from the return value of the node's function

    SerializationError
        When some of the outputs cannot be serialized with the specified Serializer
    """
    input_locations = input_locations or {}
    output_locations = output_locations or {}

    for input_name in node.inputs.keys():
        if input_name not in input_locations.keys():
            raise ValueError(
                f"This node is supposed to receive a pointer to an input named '{input_name}'. However, only the following input pointers were supplied: {list(input_locations.keys())}"
            )

    for output_name in node.outputs.keys():
        if output_name not in output_locations.keys():
            raise ValueError(
                f"This node is supposed to receive a pointer to an output named '{output_name}'. However, only the following output pointers were supplied: {list(output_locations.keys())}"
            )

    outputs = local.invoke_node(
        node,
        params={
            input_name: retrieve_input_from_location(input_location)
            for input_name, input_location in input_locations.items()
        },
    )

    for output_name, output_location in output_locations.items():
        store_output_in_location(outputs[output_name], output_location)
