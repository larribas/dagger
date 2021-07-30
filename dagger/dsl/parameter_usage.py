"""Data structures that represent how parameters are used throughout a DAG defined ."""

from typing import NamedTuple


class ParameterUsage(NamedTuple):
    """Represents the use of a parameters supplied to the DAG."""

    name: str