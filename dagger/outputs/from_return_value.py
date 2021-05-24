from dagger.serializers import DefaultSerializer, Serializer


class FromReturnValue:
    def __init__(
        self,
        serializer: Serializer = DefaultSerializer,
    ):
        self.serializer = serializer

    def from_function_return_value(self, return_value):
        return return_value
