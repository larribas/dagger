import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
from argo_workflows_sdk import DAG, DAGOutput, Node


def double(number: int) -> int:
    result = number * 2
    print(f"Doubling number {number} = {result}")
    return result


def square(number: int) -> int:
    result = number ** 2
    print(f"Squaring number {number} = {result}")
    return result


dag = DAG(
    nodes={
        "double": Node(
            double,
            inputs={
                "number": inputs.FromParam(),
            },
            outputs={
                "doubled-number": outputs.FromReturnValue(),
            },
        ),
        "square": Node(
            square,
            inputs={
                "number": inputs.FromNodeOutput(
                    node="double",
                    output="doubled-number",
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
        "number-doubled-and-squared": DAGOutput(
            node="square",
            output="squared-number",
        ),
    },
)


def run_from_cli():
    from argo_workflows_sdk.runtime.cli import invoke

    invoke(dag)
