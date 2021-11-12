"""Input retrieved from the parameters passed to the parent node."""

from typing import Optional, TypeVar

from dagger.input.empty_default_value import EmptyDefaultValue
from dagger.serializer import DefaultSerializer, Serializer

T = TypeVar("T")


class FromParam:
    """Input retrieved from the parameters passed to the parent node."""

    def __init__(
        self,
        name: Optional[str] = None,
        default_value: T = EmptyDefaultValue(),
        serializer: Serializer = DefaultSerializer,
    ):
        """
        Validate and initialize an input retrieved from a parameter.

        Parameters
        ----------
        name
            The name of the parameter in the parent node.
            If omitted, it's assumed to be equal to the name given to this input.

        default_value
            The default value, if any, of the input

        serializer
            The Serializer implementation to use to deserialize the input.

        Returns
        -------
        A valid, immutable representation of an input.
        """
        self._serializer = serializer
        self._name = name
        self._default_value = default_value

    @property
    def serializer(self) -> Serializer:
        """Get the strategy to use in order to deserialize the supplied inputs."""
        return self._serializer

    @property
    def name(self) -> Optional[str]:
        """Get the name the input references, if any."""
        return self._name

    @property
    def default_value(self) -> T:
        """Get the default value of the input references, if any."""
        return self._default_value

    def __repr__(self) -> str:
        """Get a human-readable string representation of the input."""
        return f"FromParam(name={self._name}, default_value={self._default_value}, serializer={self._serializer})"

    def __eq__(self, obj):
        """Return true if both inputs are equivalent."""
        return (
            isinstance(obj, FromParam)
            and self._name == obj._name
            and self._serializer == obj._serializer
            and self._default_value == obj._default_value
        )
