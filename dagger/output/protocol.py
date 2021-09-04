"""Protocol all outputs should conform to."""

from typing import Any, Protocol, runtime_checkable

from dagger.serializer import Serializer


@runtime_checkable
class Output(Protocol):  # pragma: no cover
    """
    Protocol all outputs conform to.

    Properties
    ----------
    serializer
        The serializer to use for the values of this output.

    is_partitioned
        A flag indicating whether this output should be partitioned. Partitioned outputs are assumed to come from an Iterable object. Each item in the Iterable should be serializable with the specified serializer.
    """

    serializer: Serializer
    is_partitioned: bool

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
