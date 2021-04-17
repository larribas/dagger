import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
from argo_workflows_sdk import DAG, DAGOutput, Node


def multiply_by_2(number: int) -> int:
    print(f"Multiplying number {number} by 2")
    return number * 2


def square(number: int) -> int:
    print(f"Squaring number {number}")
    return number ** 2


dag = DAG(
    nodes={
        "multiply-by-2": Node(
            multiply_by_2,
            inputs={
                "number": inputs.FromParam(),
            },
            outputs={
                "multiplied-number": outputs.FromReturnValue(),
            },
        ),
        "square": Node(
            square,
            inputs={
                "number": inputs.FromNodeOutput(
                    node="multiply-by-2", output="multiplied-number"
                ),
            },
            outputs={
                "squared-number": outputs.FromReturnValue(),
            },
        ),
    },
    inputs={
        "number": inputs.FromParam(),
    },
    outputs={
        "number-multiplied-by-2-and-squared": DAGOutput(
            node="square", output="squared-number"
        ),
    },
)
