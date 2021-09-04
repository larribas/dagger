"""Output retrieved from a property, when the function returns an object."""
from dagger.serializer import DefaultSerializer, Serializer


class FromProperty:
    """Output retrieved from a property, when the function returns an object."""

    def __init__(
        self,
        name: str,
        serializer: Serializer = DefaultSerializer,
        is_partitioned: bool = False,
    ):
        """
        Validate and initialize an output retrieved from a key.

        Parameters
        ----------
        name
            Name of the key that contains the output.

        serializer
            Serialization strategy to use with this output.
            If not specified, a default serializer will be used. Check the current default to be sure it will work with your data.

        is_partitioned
            A flag indicating whether this output should be partitioned. Partitioned outputs are assumed to come from an Iterable object. Each item in the Iterable should be serializable with the specified serializer.
        """
        self._name = name
        self._serializer = serializer
        self._is_partitioned = is_partitioned

    @property
    def serializer(self) -> Serializer:
        """Get the strategy to use in order to serialize the output."""
        return self._serializer

    @property
    def is_partitioned(self) -> bool:
        """Return true if the output should be partitioned."""
        return self._is_partitioned

    def from_function_return_value(self, return_value):
        """
        Retrieve the output from a property of the return value.

        Parameters
        ----------
        return_value
            An object with a property with the expected name.


        Returns
        -------
        The value that corresponds to that property.


        Raises
        ------
        TypeError
            If the function's return value does not contain the expected property.
        """
        if hasattr(return_value, self._name):
            return getattr(return_value, self._name)

        raise TypeError(
            f"This output is of type {self.__class__.__name__}. This means we expect the return value of the function to be an object with a property named '{self._name}'"
        )

    def __repr__(self) -> str:
        """Get a human-readable string representation of the output."""
        return f"FromProperty(name={self._name}, serializer={self._serializer}, is_partitioned={self._is_partitioned})"

    def __eq__(self, obj) -> bool:
        """Return true if both outputs are equivalent."""
        return (
            isinstance(obj, FromProperty)
            and self._serializer == obj._serializer
            and self._name == obj._name
        )
