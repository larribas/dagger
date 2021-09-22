import json
import os
import tempfile
import uuid

import pytest

from dagger.runtime.cli.locations import (
    PARTITION_MANIFEST_FILENAME,
    retrieve_input_from_location,
    store_output_in_location,
)
from dagger.runtime.local import OutputFile, PartitionedOutput
from dagger.serializer import DefaultSerializer


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
        file_ = _store_value_as_json(2, os.path.join(tmp, "input"))

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

        output_value = PartitionedOutput(
            [
                _store_value_as_json(1, os.path.join(tmp, "y")),
                _store_value_as_json(2, os.path.join(tmp, "x")),
            ]
        )

        store_output_in_location(
            output_location=dir_path,
            output_value=output_value,
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
        output_value = PartitionedOutput(
            [
                _store_value_as_json(
                    v,
                    os.path.join(tmp, uuid.uuid4().hex),
                )
                for v in values
            ]
        )

        store_output_in_location(
            output_location=dir_path,
            output_value=output_value,
        )

        assert (
            list(retrieve_input_from_location(dir_path, serializer=DefaultSerializer))
            == values
        )


def test__retrieve_input_from_location__reads_partitions_lazily():
    with tempfile.TemporaryDirectory() as tmp:
        dir_path = os.path.join(tmp, "partitioned_dir")
        os.mkdir(dir_path)

        output_files = [
            _store_value_as_json("original", os.path.join(dir_path, "0")),
            _store_value_as_json("original", os.path.join(dir_path, "1")),
            _store_value_as_json("original", os.path.join(dir_path, "2")),
        ]

        lazily_loaded_partitions = retrieve_input_from_location(
            dir_path,
            serializer=DefaultSerializer,
        )

        with open(output_files[0].filename, "w") as f:
            json.dump(1, f)
        assert next(lazily_loaded_partitions) == 1

        with open(output_files[1].filename, "w") as f:
            json.dump(2, f)
        assert next(lazily_loaded_partitions) == 2

        # This one hasn't been overwritten, so it should return the original value
        assert next(lazily_loaded_partitions) == "original"

        with pytest.raises(StopIteration):
            next(lazily_loaded_partitions)


def test__store_output_in_location__with_simple_output():
    with tempfile.TemporaryDirectory() as tmp:
        local_output_file = _store_value_as_json(2, os.path.join(tmp, "local"))
        cli_output_path = os.path.join(tmp, "cli")

        store_output_in_location(
            output_location=cli_output_path,
            output_value=local_output_file,
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
                    _store_value_as_json(1, os.path.join(tmp, uuid.uuid4().hex)),
                    _store_value_as_json(2, os.path.join(tmp, uuid.uuid4().hex)),
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


def test__store_output_in_location__when_file_already_exists_but_is_a_directory():
    with tempfile.TemporaryDirectory() as tmp:
        local_output_file = _store_value_as_json(
            2,
            os.path.join(tmp, uuid.uuid4().hex),
        )

        with pytest.raises(OSError):
            store_output_in_location(
                output_location=tmp,
                output_value=local_output_file,
            )


def test__store_output_in_location__when_file_already_exists():
    with tempfile.TemporaryDirectory() as tmp:
        local_output_file = _store_value_as_json(
            2,
            os.path.join(tmp, uuid.uuid4().hex),
        )
        another_output_file = _store_value_as_json(
            2,
            os.path.join(tmp, uuid.uuid4().hex),
        )

        with pytest.raises(FileExistsError):
            store_output_in_location(
                output_location=another_output_file.filename,
                output_value=local_output_file,
            )


def test__store_output_in_location__when_partition_directory_already_exists():
    with tempfile.TemporaryDirectory() as tmp:
        local_output_files = PartitionedOutput(
            [
                _store_value_as_json(1, os.path.join(tmp, uuid.uuid4().hex)),
                _store_value_as_json(2, os.path.join(tmp, uuid.uuid4().hex)),
            ],
        )

        with pytest.raises(FileExistsError):
            store_output_in_location(
                output_location=tmp,
                output_value=local_output_files,
            )


def _store_value_as_json(value, filename) -> OutputFile:
    with open(filename, "w") as f:
        json.dump(value, f)

    return OutputFile(filename=filename, serializer=DefaultSerializer)
