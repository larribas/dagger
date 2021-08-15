"""
# Static Parallelization.

This DAG shows how to define a large number of parallel steps in a DAG in a succint way.

Since DAGs are simple data structures, we can use all the power of Python to define a large number of nodes and connect them together.

## Behavior

We supply a number N to the DAG, as a parameter.

The DAG launches an arbitrarily large number (P) of parallel steps multiplying N by {1..P}.

Finally, there is a fan-in / gather step that depends on the outputs of all parallel steps. This steps sums all the results.

The DAG's output is the result of the sum operation.


## Implementation

Notice how we define `multiply_by` as a function that returns another function, and multiplies it by a number. Thus, if we called `multiply_by(5)`, the result would be the equivalent of `lambda number: number * 5`.

Next, we use a dictionary comprehension to create {1..T} tasks, each of them with a unique name and the behavior defined by `multiply_by(i)`.

Finally, we create a task named "sum-results" and, again, we use a dictionary comprehension to state that it depends on the outputs of all the tasks defined in the previous step. The `sum_results` function receives an arbitrary number of keyword arguments so that we can change the number of parallel steps without needing to change the signature of `sum_results`.

The resulting DAG will have `len(dag.nodes) == T + 1` nodes.
"""
from typing import Callable

from dagger import DAG, Task
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromReturnValue


def multiply_by(multiplier: int) -> Callable[[int], int]:
    """Given a fixed multiplier M, return a function that receives a number and multiplies it by M."""

    def multiply(number: int):
        return number * multiplier

    return multiply


def sum_results(**kwargs: int) -> int:
    """Given a list of integers supplied as keyword arguments, return their sum."""
    print(f"The results of all parallel steps, in order, were: {kwargs.values()}")
    return sum(kwargs.values())


number_of_parallel_steps = 10

dag = DAG(
    nodes={
        **{
            f"multiply-by-{i}": Task(
                multiply_by(i),
                inputs={
                    "number": FromParam(),
                },
                outputs={
                    "multiplied-number": FromReturnValue(),
                },
            )
            for i in range(number_of_parallel_steps)
        },
        "sum-results": Task(
            sum_results,
            inputs={
                f"results-from-{i}": FromNodeOutput(
                    node=f"multiply-by-{i}",
                    output="multiplied-number",
                )
                for i in range(number_of_parallel_steps)
            },
            outputs={
                "sum": FromReturnValue(),
            },
        ),
    },
    inputs={
        "number": FromParam(),
    },
    outputs={
        "sum": FromNodeOutput(
            node="sum-results",
            output="sum",
        ),
    },
)


def run_from_cli():
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dag)
