"""
# Passing Parameters.

This DAG shows how to define tasks that depend on the outputs of other tasks, or that receive their inputs from the DAG's parameters.


## Behavior

We define two tasks:
- One that multiplies a given number by 2 (that is, it doubles it).
- Another that takes that number to the power of 2 (that is, it squares it.)

The DAG receives a number as a parameter. It first doubles it, and then squares it.

The result of the DAG will be `((N*2)^2)`.


## Implementation

Note how we instruct task "double" to take its input from the DAG's parameters, and task "square" to take its input from the output of "double".

This generates a dependency from "square" to "double". All dagger runtimes will guarantee that "double" gets executed first, and forward its output to "square".
"""

import dagger.input as input
import dagger.output as output
from dagger import DAG, DAGOutput, Task


def double(number: int) -> int:  # noqa
    result = number * 2
    print(f"Doubling number {number} = {result}")
    return result


def square(number: int) -> int:  # noqa
    result = number ** 2
    print(f"Squaring number {number} = {result}")
    return result


dag = DAG(
    nodes={
        "double": Task(
            double,
            inputs={
                "number": input.FromParam(),
            },
            outputs={
                "doubled-number": output.FromReturnValue(),
            },
        ),
        "square": Task(
            square,
            inputs={
                "number": input.FromNodeOutput(
                    node="double",
                    output="doubled-number",
                ),
            },
            outputs={
                "squared-number": output.FromReturnValue(),
            },
        ),
    },
    inputs={
        "number": input.FromParam(),
    },
    outputs={
        "number-doubled-and-squared": DAGOutput(
            node="square",
            output="squared-number",
        ),
    },
)


def run_from_cli():
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dag)
