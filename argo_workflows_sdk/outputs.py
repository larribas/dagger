from argo_workflows_sdk.serializers import Serializer


class Param:
    def __init__(
        self,
        serializer: Serializer,
    ):
        self.serializer = serializer
