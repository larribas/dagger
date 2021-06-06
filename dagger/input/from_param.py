"""Input retrieved from the parameters passed to the parent node."""

from dagger.serializer import DefaultSerializer, Serializer


class FromParam:
    """Input retrieved from the parameters passed to the parent node."""

    def __init__(
        self,
        serializer: Serializer = DefaultSerializer,
    ):
        """
        Validate and initialize an input retrieved from a parameter.

        Parameters
        ----------
        serializer
            The Serializer implementation to use to deserialize the input.

        Returns
        -------
        A valid, immutable representation of an input.
        """
        self._serializer = serializer

    @property
    def serializer(self) -> Serializer:
        """Get the strategy to use in order to deserialize the supplied inputs."""
        return self._serializer
