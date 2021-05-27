from json.decoder import JSONDecodeError
from typing import Any, Optional

from dagger.serializers.errors import DeserializationError, SerializationError


class JSON:
    extension = "json"

    def __init__(
        self,
        indent: Optional[int] = None,
        allow_nan: bool = False,
    ):
        self.__indent = indent
        self.__allow_nan = allow_nan

    def serialize(self, value: Any) -> bytes:
        import json

        try:
            return json.dumps(
                value,
                indent=self.__indent,
                allow_nan=self.__allow_nan,
            ).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise SerializationError(e)

    def deserialize(self, serialized_value: bytes) -> Any:
        import json

        try:
            return json.loads(serialized_value)
        except (TypeError, JSONDecodeError) as e:
            raise DeserializationError(
                f"We cannot deserialize value '{str(serialized_value)}' as JSON. {str(e)}"
            )
