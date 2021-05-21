import re
from typing import Protocol

from argo_workflows_sdk.inputs.from_node_output import FromNodeOutput
from argo_workflows_sdk.inputs.from_param import FromParam
from argo_workflows_sdk.serializers import Serializer

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
    Input,
    # Functions
    validate_name,
    # Out-of-the-box inputs
    FromParam,
    FromNodeOutput,
]
