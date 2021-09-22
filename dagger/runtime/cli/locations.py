"""
Retrieve from / store into the specified locations.

At the moment, only locations in the local filesystem are supported.
"""

import json
import os
from typing import Any

from dagger.runtime.local import NodeOutput, PartitionedOutput
from dagger.serializer import Serializer

PARTITION_MANIFEST_FILENAME = "partitions.json"


def retrieve_input_from_location(
    input_location: str,
    serializer: Serializer,
) -> Any:
    """
    Given an input location, retrieve the contents of the file/directory it points to.

    Parameters
    ----------
    input_location
        A pointer to a path (e.g. "/my/filesystem/file.txt").
        If the path is a directory, the runtime will assume the input is partitioned,
        and concatenate all existing partitions based on the lexicographical order
        of their filenames.

    serializer
        The serializer implementation to use to deserialize the input file.


    Returns
    -------
    The original value of the input. If the input is partitioned, it returns an iterable of values.


    Raises
    ------
    FileNotFoundError
        If the file cannot be located.

    PermissionError
        If the current execution context doesn't have enough permissions to read the file.
    """
    if os.path.isdir(input_location):
        partition_filenames = [
            fname
            for fname in os.listdir(input_location)
            if os.path.isfile(os.path.join(input_location, fname))
            and fname != PARTITION_MANIFEST_FILENAME
        ]
        sorted_partition_filenames = sorted(partition_filenames, key=int)

        def load(partition_filename: str) -> Any:
            with open(os.path.join(input_location, partition_filename), "rb") as reader:
                return serializer.deserialize(reader)

        return [load(fname) for fname in sorted_partition_filenames]

    else:
        with open(input_location, "rb") as reader:
            return serializer.deserialize(reader)


def store_output_in_location(
    output_location: str,
    output_value: NodeOutput,
):
    """
    Store a serialized output into the specified location.

    It uses os.rename(): https://docs.python.org/3/library/os.html#os.rename

    Parameters
    ----------
    output_location
        A pointer to a path (e.g. "/my/filesystem/file.txt").
        The path must not exist previously.

    output_value
        A NodeOutput, pointing to the file that contains the serialized version of the output value.
        It may be partitioned. If it is, we will treat the output_location as a directory
        and dump each partition separately, together with a file named "partitions.json"
        containing a json-serialized list with all the partitions.
        Partitions filenames follow a lexicographical order, so they can be joined later
        in the same order.


    Raises
    ------
    OSError
        If the output location is a non-empty directory, in Unix

    FileExistsError
        If the output location already exists, in Windows

    IsADirectoryError
        If the output location exists and it is an empty directory, in Unix.

    PermissionError
        If the current execution context doesn't have enough permissions to read the file.
    """
    if isinstance(output_value, PartitionedOutput):
        os.mkdir(output_location)
        partition_filenames = []

        for i, src in enumerate(output_value):
            partition_filename = str(i)
            os.rename(
                src.filename,
                os.path.join(
                    output_location,
                    partition_filename,
                ),
            )
            partition_filenames.append(partition_filename)

        with open(os.path.join(output_location, PARTITION_MANIFEST_FILENAME), "w") as p:
            json.dump(partition_filenames, p)
    else:
        os.rename(output_value.filename, output_location)
