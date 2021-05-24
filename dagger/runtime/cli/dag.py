from typing import Dict

import dagger.runtime.local as local
from dagger.dag import DAG
from dagger.runtime.cli.locations import (
    retrieve_input_from_location,
    store_output_in_location,
)


def invoke(
    dag: DAG,
    input_locations: Dict[str, str] = None,
    output_locations: Dict[str, str] = None,
):
    input_locations = input_locations or {}
    output_locations = output_locations or {}

    for input_name in dag.inputs.keys():
        if input_name not in input_locations.keys():
            raise ValueError(
                f"This DAG is supposed to receive a pointer to an input named '{input_name}'. However, only the following input pointers were supplied: {list(input_locations.keys())}"
            )

    for output_name in dag.outputs.keys():
        if output_name not in output_locations.keys():
            raise ValueError(
                f"This DAG is supposed to receive a pointer to an output named '{output_name}'. However, only the following output pointers were supplied: {list(output_locations.keys())}"
            )

    outputs = local.invoke_dag(
        dag,
        params={
            input_name: retrieve_input_from_location(input_location)
            for input_name, input_location in input_locations.items()
        },
    )

    for output_name, output_location in output_locations.items():
        store_output_in_location(outputs[output_name], output_location)
