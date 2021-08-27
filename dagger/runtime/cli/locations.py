"""
Retrieve from / store into the specified locations.

At the moment, only locations in the local filesystem are supported.
"""

import json
import os

from dagger.runtime.local import NodeOutput, PartitionedOutput

PARTITION_MANIFEST_FILENAME = "partitions.json"


def retrieve_input_from_location(input_location: str) -> NodeOutput:
    """
    Given an input location, retrieve the contents of the file/directory it points to.

    Parameters
    ----------
    input_location
        A pointer to a path (e.g. "/my/filesystem/file.txt").
        If the path is a directory, the runtime will assume the input is partitioned,
        and concatenate all existing partitions based on the lexicographical order
        of their filenames.


    Returns
    -------
    The serialized version of the input. If the input is partitioned, it returns a list of serialized partitions.


    Raises
    ------
    FileNotFoundError
        If the file cannot be located.

    PermissionError
        If the current execution context doesn't have enough permissions to read the file.
    """
    if os.path.isdir(input_location):
        partition_filenames = sorted(
            [
                fname
                for fname in os.listdir(input_location)
                if os.path.isfile(os.path.join(input_location, fname))
                and fname != PARTITION_MANIFEST_FILENAME
            ]
        )

        def load_lazily(partition_filename: str):
            with open(os.path.join(input_location, partition_filename), "rb") as f:
                return f.read()

        return PartitionedOutput(map(load_lazily, partition_filenames))

    else:
        with open(input_location, "rb") as f:
            return f.read()


def store_output_in_location(output_location: str, output_value: NodeOutput):
    """
    Store a serialized output into the specified location.

    Parameters
    ----------
    output_location
        A pointer to a path (e.g. "/my/filesystem/file.txt").
        The path must not exist previously.

    output_value
        The serialized representation of a node output.
        It may be partitioned. If it is, we will treat the output_location as a directory
        and dump each partition separately, together with a file named "partitions.json"
        containing a json-serialized list with all the partitions.
        Partitions filenames follow a lexicographical order, so they can be joined later
        in the same order.


    Raises
    ------
    IsADirectoryError
        If the output_location is a directory.

    FileExistsError
        If the output_location already exists.

    PermissionError
        If the current execution context doesn't have enough permissions to read the file.
    """
    if isinstance(output_value, PartitionedOutput):
        os.mkdir(output_location)
        partition_filenames = []

        for i, partition in enumerate(output_value):
            partition_filename = str(i)
            partition_filenames.append(partition_filename)
            with open(os.path.join(output_location, partition_filename), "wb") as f:
                f.write(partition)

        with open(os.path.join(output_location, PARTITION_MANIFEST_FILENAME), "w") as p:
            json.dump(partition_filenames, p)
    else:
        with open(output_location, "wb") as f:
            f.write(output_value)
