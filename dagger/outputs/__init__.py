import re
from typing import Protocol

from dagger.outputs.from_key import FromKey
from dagger.outputs.from_property import FromProperty
from dagger.outputs.from_return_value import FromReturnValue
from dagger.serializers import Serializer

VALID_NAME_REGEX = r"^[a-zA-Z0-9][a-zA-Z0-9-_]{0,63}$"
VALID_NAME = re.compile(VALID_NAME_REGEX)


class Output(Protocol):
    serializer: Serializer

    def from_function_return_value(self, results):
        ...


def validate_name(name: str):
    if not VALID_NAME.match(name):
        raise ValueError(
            f"'{name}' is not a valid name for an output. Outputs must comply with the regex {VALID_NAME_REGEX}"
        )


__all__ = [
    Output,
    # Functions
    validate_name,
    # Out-of-the-box outputs
    FromReturnValue,
    FromKey,
    FromProperty,
]
