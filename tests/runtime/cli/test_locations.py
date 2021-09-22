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
from dagger.serializer import DefaultSerializer
from tests.runtime.cli.utils import store_value


def test__retrieve_input_from_location__when_location_doesnt_exist():
    with tempfile.TemporaryDirectory() as tmp:
        input_file = os.path.join(tmp, "input")

        with pytest.raises(FileNotFoundError):
            retrieve_input_from_location(
                input_location=input_file,
                serializer=DefaultSerializer,
            )


def test__retrieve_input_from_location__pointing_to_a_file():
    with tempfile.TemporaryDirectory() as tmp:
        file_ = store_value(2, tmp)

        assert (
            retrieve_input_from_location(
                input_location=file_.filename,
                serializer=file_.serializer,
            )
            == 2
        )


def test__retrieve_input_from_location__can_read_partitioned_directory():
    with tempfile.TemporaryDirectory() as tmp:
        dir_path = os.path.join(tmp, "partitioned_dir")

        store_output_in_location(
            output_location=dir_path,
            output_value=PartitionedOutput(
                [
                    store_value(1, tmp),
                    store_value(2, tmp),
                ]
            ),
        )

        assert list(
            retrieve_input_from_location(dir_path, serializer=DefaultSerializer)
        ) == [1, 2]


def test__retrieve_input_from_location__sorts_partitions_in_the_same_order_they_were_stored():
    with tempfile.TemporaryDirectory() as tmp:
        dir_path = os.path.join(tmp, "partitioned_dir")

        # We're specifically testing the order of 11 partitions whose filenames
        # are named 1..11. When sorting them lexicographically, 2 > 10.
        # Instead, we need to sort them numerically.
        values = list(range(11))

        store_output_in_location(
            output_location=dir_path,
            output_value=PartitionedOutput([store_value(v, tmp) for v in values]),
        )

        assert (
            list(retrieve_input_from_location(dir_path, serializer=DefaultSerializer))
            == values
        )


def test__store_output_in_location__with_simple_output():
    with tempfile.TemporaryDirectory() as tmp:
        cli_output_path = os.path.join(tmp, "cli")

        store_output_in_location(
            output_location=cli_output_path,
            output_value=store_value(2, tmp),
        )

        with open(cli_output_path, "rb") as f:
            assert f.read() == b"2"


def test__store_output_in_location__with_partitioned_output():
    with tempfile.TemporaryDirectory() as tmp:
        output_path = os.path.join(tmp, "output")
        store_output_in_location(
            output_location=output_path,
            output_value=PartitionedOutput(
                [
                    store_value(1, tmp),
                    store_value(2, tmp),
                ]
            ),
        )

        with open(os.path.join(output_path, PARTITION_MANIFEST_FILENAME), "r") as f:
            partition_filenames = json.load(f)

        partitions = []
        for partition_filename in partition_filenames:
            with open(os.path.join(output_path, partition_filename), "rb") as f:
                partitions.append(f.read())

        assert partitions == [b"1", b"2"]
