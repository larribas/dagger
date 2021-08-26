"""
# Map-Reduce.

This DAG shows how to create a dynamic fan-out and fan-in operation.

We use dagger's output and node partitioning capabilities to achieve that.

## Behavior

We supply a multiplier M to the DAG, as a parameter.

The DAG generates an arbitrarily long list of numbers.

Then, we map each of these numbers separately through a partitioned task, and multiply each of them by the multiplier M.

Finally, there is a fan-in / gather step that depends on the outputs of all parallel steps. This steps sums all the results.

The DAG's output is the result of the sum operation.


## Implementation

Notice how the output of "generate-numbers" is partitioned. This will cause each of the items in the list to be stored separately.

The "multiply-by" task is partitioned by the numbers, which means there will be P executions of the "multiply-by" task, each of them processing one of the numbers.

After all the instances of "multiply-by" are done, we have a fan-in step ("sum-results", which is not partitioned on purpose) that will depend on the outputs of "multiply-by". Because this task is not partitioned, it will receive a list with all the results of the executions of "multiply-by" as a single list. It will just sum all the partitions together and produce a single result.
"""
import random
from typing import List

from dagger import DAG, Task
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromReturnValue


def generate_numbers() -> List[int]:
    """Multiply 'number' by 'multiplier'."""
    return [i for i in range(random.randint(1, 10))]


def multiply_by(multiplier: int, number: int) -> int:
    """Multiply 'number' by 'multiplier'."""
    return number * multiplier


def sum_results(numbers: List[int]) -> int:
    """Given a list of integers, return their sum."""
    print(f"The results of all parallel steps, in order, were: {numbers}")
    return sum(numbers)


dag = DAG(
    inputs={"multiplier": FromParam()},
    outputs={
        "sum": FromNodeOutput("sum-results", "sum"),
    },
    nodes={
        "generate-numbers": Task(
            generate_numbers,
            outputs={
                "numbers": FromReturnValue(is_partitioned=True),
            },
        ),
        "multiply-by": Task(
            multiply_by,
            inputs={
                "multiplier": FromParam(),
                "number": FromNodeOutput("generate-numbers", "numbers"),
            },
            outputs={
                "number": FromReturnValue(is_partitioned=True),
            },
            partition_by_input="number",
        ),
        "sum-results": Task(
            sum_results,
            inputs={
                "numbers": FromNodeOutput("multiply-by", "number"),
            },
            outputs={"sum": FromReturnValue()},
        ),
    },
)


def run_from_cli():
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dag)
