"""Input retrieved from the output of another node."""

from dagger.serializer import DefaultSerializer, Serializer


class FromNodeOutput:
    """Input retrieved from the output of another node."""

    def __init__(
        self,
        node: str,
        output: str,
        serializer: Serializer = DefaultSerializer,
    ):
        """
        Validate and initialize an input pointing to the output of a different node.

        Parameters
        ----------
        node
            The name of the other node.

        output
            The name of the output declared by 'node'

        serializer
            The Serializer implementation to use to deserialize the input.


        Returns
        -------
        A valid, immutable representation of an input.
        """
        self._node_name = node
        self._node_output_name = output
        self._serializer = serializer

    @property
    def node(self) -> str:
        """Get the name of the node the input should be retrieved from."""
        return self._node_name

    @property
    def output(self) -> str:
        """Get the name of the output the input should be retrieved from."""
        return self._node_output_name

    @property
    def serializer(self) -> Serializer:
        """Get the strategy to use in order to deserialize the supplied inputs."""
        return self._serializer
