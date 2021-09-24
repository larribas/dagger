import os
import shutil
import tempfile
import tracemalloc
import uuid
from typing import Any, BinaryIO

import pytest

KB = 1024
MB = KB ** 2


class FileSerializer:
    """
    Serialize a data structured backed by the file system.

    This class is a minimal example to prove that it is possible to serialize
    a native Python type that is backed by the file system instead of by memory.

    Conceptually, it assumes the values to serialize are filenames
    (pointers to files in the local file system), and the result is
    the binary representation of that file.

    When deserialized, it produces a different pointer to a file
    within the specified output directory.

    If the serialization and deserialization operations can maintain a constant
    use of memory, it is proof that we can write serializers for other data
    structures such as Dask's DataFrames, which are backed by a directory of
    dataframe partitions.
    """

    extension = "file"

    def __init__(self, output_dir_path: str):
        self._output_dir_path = output_dir_path

    def serialize(self, value: Any, writer: BinaryIO):
        with open(value, "rb") as src:
            shutil.copyfileobj(src, writer)

    def deserialize(self, reader: BinaryIO) -> Any:
        filename = os.path.join(self._output_dir_path, uuid.uuid4().hex)
        with open(filename, "wb") as dst:
            shutil.copyfileobj(reader, dst)

        return filename


def test_memory_footprint_can_be_constant():
    with tempfile.TemporaryDirectory() as tmp:
        large_file = os.path.join(tmp, "large_file")
        serialized_file = os.path.join(tmp, "serialized_file")

        # Write a 100-MB file
        with open(large_file, "w") as f:
            for _ in range(100 * KB):
                f.write("x" * KB)

        tracemalloc.start()
        serializer = FileSerializer(tmp)
        snapshot_before_serialization = tracemalloc.take_snapshot()

        with open(serialized_file, "wb") as f:
            serializer.serialize(large_file, f)

        snapshot_after_serialization = tracemalloc.take_snapshot()

        with open(serialized_file, "rb") as f:
            deserialized_filename = serializer.deserialize(f)

        assert deserialized_filename not in [large_file, serialized_file]

        snapshot_after_deserialization = tracemalloc.take_snapshot()

        largest_memory_allocation_when_serializing = (
            snapshot_after_serialization.compare_to(
                snapshot_before_serialization,
                "lineno",
            )[0].size_diff
        )
        assert largest_memory_allocation_when_serializing < MB

        largest_memory_allocation_when_deserializing = (
            snapshot_after_deserialization.compare_to(
                snapshot_after_serialization,
                "lineno",
            )[0].size_diff
        )
        assert largest_memory_allocation_when_deserializing < MB
