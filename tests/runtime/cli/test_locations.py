import json
import os
import tempfile

import pytest

from dagger.runtime.cli.locations import (
    PARTITION_MANIFEST_FILENAME,
    retrieve_input_from_location,
    store_output_in_location,
)
from dagger.runtime.local import PartitionedOutput


def test__retrieve_input_from_location__when_location_doesnt_exist():
    with tempfile.TemporaryDirectory() as tmp:
        input_file = os.path.join(tmp, "input")

        with pytest.raises(FileNotFoundError):
            retrieve_input_from_location(input_file)


def test__retrieve_input_from_location__pointing_to_a_file():
    with tempfile.TemporaryDirectory() as tmp:
        input_file = os.path.join(tmp, "input")

        with open(input_file, "wb") as f:
            f.write(b"2")

        assert retrieve_input_from_location(input_file) == b"2"


def test__retrieve_input_from_location__can_read_partitioned_directory():
    with tempfile.TemporaryDirectory() as tmp:
        dir_path = os.path.join(tmp, "partitioned_dir")

        partitions = [b"1", b"2"]
        store_output_in_location(
            output_location=dir_path,
            output_value=PartitionedOutput(partitions),
        )

        assert list(retrieve_input_from_location(dir_path)) == partitions


def test__retrieve_input_from_location__sorts_partitions_in_the_same_order_they_were_stored():
    with tempfile.TemporaryDirectory() as tmp:
        dir_path = os.path.join(tmp, "partitioned_dir")

        # We're specifically testing the order of 11 partitions whose filenames
        # are named 1..11. When sorting them lexicographically, 2 > 10.
        # Instead, we need to sort them numerically.
        partitions = [bytes(i) for i in range(11)]
        store_output_in_location(
            output_location=dir_path,
            output_value=PartitionedOutput(partitions),
        )

        assert list(retrieve_input_from_location(dir_path)) == partitions


def test__retrieve_input_from_location__reads_partitions_lazily():
    with tempfile.TemporaryDirectory() as tmp:
        dir_path = os.path.join(tmp, "partitioned_dir")
        os.mkdir(dir_path)

        for c in ["0", "1", "2"]:
            with open(os.path.join(dir_path, c), "wb") as f:
                # Create the file but do not write anything yet
                f.write(b"")

        lazily_loaded_partitions = retrieve_input_from_location(dir_path)

        with open(os.path.join(dir_path, "0"), "wb") as f:
            f.write(b"1")
        assert next(lazily_loaded_partitions) == b"1"

        with open(os.path.join(dir_path, "1"), "wb") as f:
            f.write(b"2")
        assert next(lazily_loaded_partitions) == b"2"

        # This one hasn't been written, so it should return an empty bytestring
        assert next(lazily_loaded_partitions) == b""

        with pytest.raises(StopIteration):
            next(lazily_loaded_partitions)


def test__retrieve_input_from_location__can_read_partitioned_directory_without_a_partitions_manifest():
    with tempfile.TemporaryDirectory() as tmp:
        dir_path = os.path.join(tmp, "partitioned_dir")
        os.mkdir(dir_path)

        partitions = [b"1", b"2", b"3"]
        for i, partition in enumerate(partitions):
            with open(os.path.join(dir_path, str(i)), "wb") as f:
                f.write(partition)

        assert list(retrieve_input_from_location(dir_path)) == partitions


def test__store_output_in_location__with_simple_output():
    with tempfile.TemporaryDirectory() as tmp:
        output_path = os.path.join(tmp, "output")
        store_output_in_location(
            output_location=output_path,
            output_value=b"2",
        )

        with open(output_path, "rb") as f:
            assert f.read() == b"2"


def test__store_output_in_location__with_partitioned_output():
    with tempfile.TemporaryDirectory() as tmp:
        output_path = os.path.join(tmp, "output")
        store_output_in_location(
            output_location=output_path,
            output_value=PartitionedOutput([b"1", b"2"]),
        )

        with open(os.path.join(output_path, PARTITION_MANIFEST_FILENAME), "r") as f:
            partition_filenames = json.load(f)

        partitions = []
        for partition_filename in partition_filenames:
            with open(os.path.join(output_path, partition_filename), "rb") as f:
                partitions.append(f.read())

        assert partitions == [b"1", b"2"]


def test__store_output_in_location__when_file_already_exists_but_is_a_directory():
    with tempfile.TemporaryDirectory() as tmp:
        # Although we generally expect an IsADirectoryError, Python captures
        # a PermissionError on Windows due to a bug:
        # https://bugs.python.org/issue43095
        with pytest.raises((IsADirectoryError, PermissionError)):
            store_output_in_location(
                output_location=tmp,
                output_value=b"2",
            )


def test__store_output_in_location__when_partition_directory_already_exists():
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(FileExistsError):
            store_output_in_location(
                output_location=tmp,
                output_value=PartitionedOutput([b"2"]),
            )
