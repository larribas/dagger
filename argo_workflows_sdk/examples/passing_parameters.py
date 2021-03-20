import random

import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
import argo_workflows_sdk.serializers as serializers
from argo_workflows_sdk import DAG, Node


def generate_random_number() -> int:
    number = random.randint(1, 1000)
    print(f"I've generated the number {number}")
    return number


def echo_number(number: int):
    print(f"I received the number {number}!")


dag = DAG(
    "passing_parameters",
    [
        Node(
            "generate-random-number",
            generate_random_number,
            outputs={
                "number": outputs.Param(serializers.JSON()),
            },
        ),
        Node(
            "echo-number",
            echo_number,
            inputs={
                "number": inputs.FromOutput(
                    node="generate-random-number", output="number"
                )
            },
        ),
    ],
)
