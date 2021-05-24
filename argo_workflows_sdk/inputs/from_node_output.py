from argo_workflows_sdk.serializers import DefaultSerializer, Serializer


class FromNodeOutput:
    def __init__(
        self,
        node: str,
        output: str,
        serializer: Serializer = DefaultSerializer,
    ):
        self.node = node
        self.output = output
        self.serializer = serializer
