"""Protocol all inputs should conform to."""

from typing import Protocol, runtime_checkable

from dagger.serializer import Serializer


@runtime_checkable
class Input(Protocol):
    """Protocol all inputs conform to."""

    serializer: Serializer
