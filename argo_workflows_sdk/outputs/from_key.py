from typing import Any, Dict

from argo_workflows_sdk.serializers import DefaultSerializer, Serializer


class FromKey:
    def __init__(
        self,
        name: str,
        serializer: Serializer = DefaultSerializer,
    ):
        self.name = name
        self.serializer = serializer

    def from_function_return_value(self, return_value: Dict[str, Any]) -> Any:
        if isinstance(return_value, dict):
            try:
                return return_value[self.name]
            except KeyError:
                raise ValueError(
                    f"An output of type {self.__class__.__name__}('{self.name}') expects the return value of the function to be a dictionary containing, at least, a key named '{self.name}'"
                )

        raise TypeError(
            f"This output is of type {self.__class__.__name__}. This means we expect the return value of the function to be a dictionary containing, at least, a key named '{self.name}'"
        )
