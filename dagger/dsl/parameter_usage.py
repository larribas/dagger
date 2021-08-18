"""Data structures that represent how parameters are used throughout a DAG defined ."""

from typing import NamedTuple, Optional

from dagger.serializer import DefaultSerializer, Serializer


class ParameterUsage(NamedTuple):
    """Represents the use of a parameters supplied to the DAG."""

    name: Optional[str] = None
    serializer: Serializer = DefaultSerializer
