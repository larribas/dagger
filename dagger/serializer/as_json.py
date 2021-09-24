"""Serialization strategy based on JSON."""

import io
from json.decoder import JSONDecodeError
from typing import Any, BinaryIO, Optional

from dagger.serializer.errors import DeserializationError, SerializationError


class AsJSON:
    """Serializer implementation that uses JSON to marshal/unmarshal Python data structures."""

    extension = "json"

    def __init__(
        self,
        indent: Optional[int] = None,
        allow_nan: bool = False,
    ):
        """
        Initialize a JSON serializer.

        Parameters
        ----------
        indent: int, optional
            Set the indentation to format the json with.
            This may come in handy in some situations if you need to debug/troubleshoot an issue with parameters that are passed from one task to another. However, note that extra indentation makes the serialized payload heavier. Thus, we don't recommend setting this in a production environment.

        allow_nan: bool
            Whether or not to allow NaN values.
            See the official json library in Python for more details about the expected behavior.
        """
        self._indent = indent
        self._allow_nan = allow_nan

    def serialize(self, value: Any, writer: BinaryIO):
        """
        Serialize a value into a JSON object, encoded into binary format using utf-8.

        The value needs to be serializable into JSON by the standard 'json' library in Python.
        """
        import json

        try:
            json.dump(
                value,
                io.TextIOWrapper(writer, encoding="utf-8"),
                indent=self._indent,
                allow_nan=self._allow_nan,
            )
        except (TypeError, ValueError) as e:
            raise SerializationError(e)

    def deserialize(self, reader: BinaryIO) -> Any:
        """Deserialize a utf-8-encoded json object into the value it represents."""
        import json

        try:
            return json.load(reader)
        except (TypeError, JSONDecodeError) as e:
            raise DeserializationError(e)

    def __repr__(self) -> str:
        """Get a human-readable string representation of the serializer."""
        return f"AsJSON(indent={self._indent}, allow_nan={self._allow_nan})"

    def __eq__(self, obj) -> bool:
        """Return true if both serializers are equivalent."""
        return (
            isinstance(obj, AsJSON)
            and self._indent == obj._indent
            and self._allow_nan == obj._allow_nan
        )
