import re
from typing import Protocol

from dagger.inputs.from_node_output import FromNodeOutput
from dagger.inputs.from_param import FromParam
from dagger.serializers import Serializer

VALID_NAME_REGEX = r"^[a-zA-Z0-9][a-zA-Z0-9-_]{0,63}$"
VALID_NAME = re.compile(VALID_NAME_REGEX)


class Input(Protocol):
    serializer: Serializer


def validate_name(name: str):
    if not VALID_NAME.match(name):
        raise ValueError(
            f"'{name}' is not a valid name for an input. Inputs must comply with the regex {VALID_NAME_REGEX}"
        )


__all__ = [
    Input.__name__,
    # Functions
    validate_name.__name__,
    # Out-of-the-box inputs
    FromParam.__name__,
    FromNodeOutput.__name__,
]
