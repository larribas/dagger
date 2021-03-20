import re
from typing import Callable, Dict, Union

import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs

VALID_NAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,128}$")


class Node:
    def __init__(
        self,
        name: str,
        func: Callable,
        inputs: Dict[str, Union[inputs.FromOutput]] = {},
        outputs: Dict[str, Union[outputs.Param]] = {},
    ):
        # TODO: Guarantee that names are valid
        self.name = name
        self.func = func
        self.inputs = inputs
        self.outputs = outputs

    def __call__(self):
        return self.func()
