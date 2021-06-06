"""Run tasks taking their inputs and outputs from files."""

from typing import Mapping

import dagger.runtime.local as local
from dagger.runtime.cli.locations import (
    retrieve_input_from_location,
    store_output_in_location,
)
from dagger.task import Task


def invoke_task(
    task: Task,
    input_locations: Mapping[str, str] = None,
    output_locations: Mapping[str, str] = None,
):
    """
    Invoke a task. Take the inputs from and store the outputs in the locations specified.

    Parameters
    ----------
    task
        Task to execute

    input_locations
        Location of the files that contain the values for the inputs of the task.
        The contents of the files must be serialized in a binary format.
        The mapping's keys are the names of the inputs.

    output_locations
        Location of the files where the outputs of the task should be stored.
        The contents of the files will be serialized in a binary format.
        The mapping's keys are the names of the outputs.


    Raises
    ------
    ValueError
        When the location of any required input/output is missing

    TypeError
        When any of the outputs cannot be obtained from the return value of the task's function

    SerializationError
        When some of the outputs cannot be serialized with the specified Serializer
    """
    input_locations = input_locations or {}
    output_locations = output_locations or {}

    for input_name in task.inputs:
        if input_name not in input_locations:
            raise ValueError(
                f"This task is supposed to receive a pointer to an input named '{input_name}'. However, only the following input pointers were supplied: {list(input_locations)}"
            )

    for output_name in task.outputs:
        if output_name not in output_locations:
            raise ValueError(
                f"This task is supposed to receive a pointer to an output named '{output_name}'. However, only the following output pointers were supplied: {list(output_locations)}"
            )

    outputs = local.invoke_task(
        task,
        params={
            input_name: retrieve_input_from_location(input_locations[input_name])
            for input_name in input_locations
        },
    )

    for output_name in output_locations:
        store_output_in_location(outputs[output_name], output_locations[output_name])
