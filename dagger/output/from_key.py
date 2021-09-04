"""Output retrieved from a key, when the function returns a Mapping."""

from typing import Generic, Mapping, TypeVar

from dagger.serializer import DefaultSerializer, Serializer

K = TypeVar("K")
V = TypeVar("V")


class FromKey(Generic[K, V]):
    """Output retrieved from a key, when the function returns a Mapping."""

    def __init__(
        self,
        name: K,
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

    def from_function_return_value(self, return_value: Mapping[K, V]) -> V:
        """
        Retrieve the output from the return value of the task's function.

        Parameters
        ----------
        return_value
            A mapping returned by the Task's function


        Returns
        -------
        The value that corresponds to that key


        Raises
        ------
        ValueError
            If the function's return value does not contain the specified key.

        TypeError
            If the function's return value is not a mapping.
        """
        if isinstance(return_value, Mapping):
            try:
                return return_value[self._name]
            except KeyError:
                raise ValueError(
                    f"An output of type {self.__class__.__name__}('{self._name}') expects the return value of the function to be a mapping containing, at least, a key named '{self._name}'"
                )

        raise TypeError(
            f"This output is of type {self.__class__.__name__}. This means we expect the return value of the function to be a mapping containing, at least, a key named '{self._name}'"
        )

    def __repr__(self) -> str:
        """Get a human-readable string representation of the output."""
        return f"FromKey(key={self._name}, serializer={self._serializer}, is_partitioned={self._is_partitioned})"

    def __eq__(self, obj) -> bool:
        """Return true if both outputs are equivalent."""
        return (
            isinstance(obj, FromKey)
            and self._serializer == obj._serializer
            and self._name == obj._name
        )
