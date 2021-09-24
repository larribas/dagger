import tracemalloc
tracemalloc.start()

from typing import Any, BinaryIO

import os
import shutil
import uuid

class FileSerializer:
    extension = "custom"

    def __init__(self, output_dir_path: str):
        self._output_dir_path = output_dir_path

    def serialize(self, value: Any, writer: BinaryIO):
        # Value is a filename
        with open(value, "rb") as src:
            shutil.copyfileobj(src, writer)

    def deserialize(self, reader: BinaryIO) -> Any:
        filename = os.path.join(self._output_dir_path, uuid.uuid4().hex)
        with open(filename, "wb") as dst:
            shutil.copyfileobj(reader, dst)

        return filename
        

serializer = FileSerializer(".")
filename = "bigfile.txt"

snapshot1 = tracemalloc.take_snapshot()


serialized_filename = "serialized"

with open(serialized_filename, "wb") as f:
    serializer.serialize(filename, f)

snapshot2 = tracemalloc.take_snapshot()


with open(serialized_filename, "rb") as f:
    deserialized_filename = serializer.deserialize(f)

print(f"Deserialized filename {deserialized_filename}")

snapshot3 = tracemalloc.take_snapshot()

stats = snapshot2.compare_to(snapshot1, 'lineno')
print(stats[0].size_diff)

stats = snapshot3.compare_to(snapshot2, 'lineno')
print(stats[0].size_diff)
