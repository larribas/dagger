"""Protocol all outputs should conform to."""

from typing import Any, Protocol, runtime_checkable

from dagger.serializer import Serializer


@runtime_checkable
class Output(Protocol):
    """Protocol all outputs conform to."""

    serializer: Serializer

    def from_function_return_value(self, return_value: Any) -> Any:
        """
        Given the results of a function, retrieve the value for the output.

        Parameters
        ----------
        return_value
            The return value of the function.


        Returns
        -------
        The value for this output.


        Raises
        ------
        ValueError
            If the output cannot be retrieved from the function's return value.

        TypeError
            If the function's return value has an unexpected type.
        """
        ...
