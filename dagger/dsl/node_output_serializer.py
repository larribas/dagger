"""Annotate types to set specific serializers."""

from typing import Optional

from dagger.serializer import DefaultSerializer, Serializer


class NodeOutputSerializer:
    """Indicate the serializer that should be used for the outputs of a specific node."""

    def __init__(self, root: Serializer = DefaultSerializer, **kwargs: Serializer):
        self._root = root
        self._sub_outputs = kwargs

    @property
    def root(self) -> Serializer:
        """Return the serializer for the root output of the function."""
        return self._root

    def sub_output(self, output_name: str) -> Optional[Serializer]:
        """Return the serializer assigned to the output with the name provided, if any."""
        return self._sub_outputs.get(output_name, None)

    def __eq__(self, obj) -> bool:
        """Return true if the object is equivalent to the current instance."""
        return (
            isinstance(obj, NodeOutputSerializer)
            and self._root == obj._root
            and self._sub_outputs == obj._sub_outputs
        )

    def __repr__(self) -> str:
        """Return a human-readable representation of this class."""
        kv_serializers = [f"{k}={v}" for k, v in self._sub_outputs.items()]
        all_serializers = ", ".join([f"root={self._root}"] + kv_serializers)
        return f"Serialize({all_serializers})"
