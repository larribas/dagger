"""Data structures that represent how parameters are used throughout a DAG defined ."""
from typing import Union

from dagger.input import EmptyDefaultValue
from dagger.serializer import DefaultSerializer, JSONSerializableType, Serializer


class ParameterUsage:
    """Represents the use of a parameters supplied to the DAG."""

    def __init__(
        self,
        name: str,
        default_value: Union[
            EmptyDefaultValue, JSONSerializableType
        ] = EmptyDefaultValue(),
        serializer: Serializer = DefaultSerializer,
    ):
        self._name = name
        self._default_value = default_value
        self._serializer = serializer

    @property
    def name(self) -> str:
        """Return the name of the parameter."""
        return self._name

    @property
    def default_value(self) -> Union[EmptyDefaultValue, JSONSerializableType]:
        """Return the default value of the parameter."""
        return self._default_value

    @property
    def serializer(self) -> Serializer:
        """Return the serializer assigned to this parameter."""
        return self._serializer

    def __iter__(self):
        """Attempt to iterate on a parameter. At the moment, this is not valid in Dagger."""
        raise ValueError(
            "Iterating over the value of a parameter is not a valid parallelization pattern in Dagger. You need to convert the parameter into the output of a node. Read this section in the documentation to find out more: https://larribas.me/dagger/user-guide/map-reduce/"
        )

    def __repr__(self) -> str:
        """Get a human-readable string representation of this parameter usage."""
        return f"ParameterUsage(name={self._name}, default_value={self._default_value}, serializer={self._serializer})"

    def __eq__(self, obj) -> bool:
        """Return true if both objects are equivalent."""
        return (
            isinstance(obj, ParameterUsage)
            and self._name == obj._name
            and self._serializer == obj._serializer
            and self._default_value == obj._default_value
        )
