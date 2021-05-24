from dagger.serializers import DefaultSerializer, Serializer


class FromParam:
    def __init__(
        self,
        serializer: Serializer = DefaultSerializer,
    ):
        self.serializer = serializer
