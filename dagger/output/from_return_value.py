"""Output retrieved directly from the return value of the task's function."""

from typing import Generic, TypeVar

from dagger.serializer import DefaultSerializer, Serializer

T = TypeVar("T")


class FromReturnValue(Generic[T]):
    """Output retrieved directly from the return value of the task's function."""

    def __init__(
        self,
        serializer: Serializer = DefaultSerializer,
    ):
        """
        Validate and initialize an output retrieved from a key.

        Parameters
        ----------
        serializer
            Serialization strategy to use with this output.
            If not specified, a default serializer will be used. Check the current default to be sure it will work with your data.
        """
        self._serializer = serializer

    @property
    def serializer(self) -> Serializer:
        """Get the strategy to use in order to serialize the output."""
        return self._serializer

    def from_function_return_value(self, return_value: T) -> T:
        """Retrieve the output from the return value of the task's function."""
        return return_value

    def __eq__(self, obj) -> bool:
        """Return true if both outputs are equivalent."""
        return isinstance(obj, FromReturnValue) and self._serializer == obj._serializer

    def __str__(self) -> str:
        """Return a human-readable representation of the output."""
        return f"FromReturnValue(serializer={self._serializer})"
