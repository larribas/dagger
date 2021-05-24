from typing import Callable, List

import dagger.inputs as inputs
import dagger.outputs as outputs
from dagger import DAG, DAGOutput, Node


def multiply_by(multiplier: int) -> Callable[[int], int]:
    def multiply(number: int):
        return number * multiplier

    return multiply


def sum_results(**kwargs) -> int:
    print(f"The results of all parallel steps, in order, were: {kwargs.values()}")
    return sum(kwargs.values())


number_of_parallel_steps = 10

dag = DAG(
    nodes={
        **{
            f"multiply-by-{i}": Node(
                multiply_by(i),
                inputs={
                    "number": inputs.FromParam(),
                },
                outputs={
                    "multiplied-number": outputs.FromReturnValue(),
                },
            )
            for i in range(number_of_parallel_steps)
        },
        "sum-results": Node(
            sum_results,
            inputs={
                f"results-from-{i}": inputs.FromNodeOutput(
                    node=f"multiply-by-{i}",
                    output="multiplied-number",
                )
                for i in range(number_of_parallel_steps)
            },
            outputs={
                "sum": outputs.FromReturnValue(),
            },
        ),
    },
    inputs={
        "number": inputs.FromParam(),
    },
    outputs={
        "sum": DAGOutput(
            node="sum-results",
            output="sum",
        ),
    },
)


def run_from_cli():
    from dagger.runtime.cli import invoke

    invoke(dag)
