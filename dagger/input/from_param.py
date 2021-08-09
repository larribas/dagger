"""Input retrieved from the parameters passed to the parent node."""

from typing import Optional

from dagger.serializer import DefaultSerializer, Serializer


class FromParam:
    """Input retrieved from the parameters passed to the parent node."""

    def __init__(
        self,
        name: Optional[str] = None,
        serializer: Serializer = DefaultSerializer,
    ):
        """
        Validate and initialize an input retrieved from a parameter.

        Parameters
        ----------
        serializer
            The Serializer implementation to use to deserialize the input.

        name
            The name of the parameter in the parent node.
            If omitted, it's assumed to be equal to the name given to this input.

        Returns
        -------
        A valid, immutable representation of an input.
        """
        self._serializer = serializer
        self._name = name

    @property
    def serializer(self) -> Serializer:
        """Get the strategy to use in order to deserialize the supplied inputs."""
        return self._serializer

    @property
    def name(self) -> Optional[str]:
        """Get the name the input references, if any."""
        return self._name

    def __repr__(self) -> str:
        """Get a human-readable string representation of the input."""
        return f"FromParam(name={self._name}, serializer={self._serializer})"

    def __eq__(self, obj):
        """Return true if both inputs are equivalent."""
        return (
            isinstance(obj, FromParam)
            and self._name == obj._name
            and self._serializer == obj._serializer
        )
