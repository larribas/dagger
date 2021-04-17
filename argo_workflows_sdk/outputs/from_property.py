from argo_workflows_sdk.serializers import DefaultSerializer, Serializer


class FromProperty:
    def __init__(
        self,
        name: str,
        serializer: Serializer = DefaultSerializer,
    ):
        self.name = name
        self.serializer = serializer

    def from_function_return_value(self, return_value):
        if hasattr(return_value, self.name):
            return getattr(return_value, self.name)

        raise TypeError(
            f"This output is of type {self.__class__.__name__}. This means we expect the return value of the function to be an object with a property named '{self.name}'"
        )
