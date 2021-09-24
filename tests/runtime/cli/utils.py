"""Utilities to test the CLI runtime."""

import os
import uuid
from typing import Any, Optional

from dagger.runtime.local import OutputFile
from dagger.serializer import DefaultSerializer, Serializer


def store_value(
    value: Any,
    dirname: str,
    filename: Optional[str] = None,
    serializer: Serializer = DefaultSerializer,
) -> OutputFile:
    """Store the value serialized as json in the directory."""
    filename = filename or uuid.uuid4().hex
    path = os.path.join(dirname, filename)
    with open(path, "wb") as f:
        serializer.serialize(value, f)

    return OutputFile(
        filename=path,
        serializer=serializer,
    )
