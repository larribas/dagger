from typing import Any, Optional

from argo_workflows_sdk.serializers.errors import (
    DeserializationError,
    SerializationError,
)


class JSON:
    extension = "json"

    def __init__(
        self,
        indent: Optional[int] = None,
        allow_nan: Optional[bool] = False,
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
        except TypeError as e:
            raise DeserializationError(e)
