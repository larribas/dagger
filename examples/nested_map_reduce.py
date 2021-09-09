"""
# Nested Map-Reduce.

This DAG shows how to create two dynamic fan-out and fan-in operations, one nested within the other.

See the simpler map-reduce example first.
"""
from typing import List

from dagger import DAG, Task
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromReturnValue


def fan_out(parallel_steps: int) -> List[int]:
    """Generate a list of numbers from 0 to 'parallel_steps'."""
    numbers = [i for i in range(parallel_steps)]
    print(f"Numbers: {numbers}")
    return numbers


def multiply_by(multiplier: int, number: int) -> int:
    """Multiply 'number' by 'multiplier'."""
    result = number * multiplier
    print(f"Result: {result}")
    return result


def sum_results(numbers: List[int]) -> int:
    """Given a list of integers, return their sum."""
    print(f"The results of all parallel steps, in order, were: {numbers}")
    return sum(numbers)


dag = DAG(
    inputs={
        "multiplier": FromParam(),
        "parallel_steps": FromParam(),
    },
    outputs={
        "sum": FromNodeOutput("reduce", "sum"),
    },
    nodes={
        "fan-out": Task(
            fan_out,
            inputs={
                "parallel_steps": FromParam(),
            },
            outputs={
                "numbers": FromReturnValue(is_partitioned=True),
            },
        ),
        "map": DAG(
            inputs={
                "number": FromNodeOutput("fan-out", "numbers"),
                "multiplier": FromParam(),
            },
            partition_by_input="number",
            outputs={
                "result": FromNodeOutput("map-2", "number"),
            },
            nodes={
                "map-1": Task(
                    multiply_by,
                    inputs={
                        "multiplier": FromParam(),
                        "number": FromParam(),
                    },
                    outputs={
                        "number": FromReturnValue(),
                    },
                ),
                "map-2": Task(
                    multiply_by,
                    inputs={
                        "multiplier": FromParam(),
                        "number": FromNodeOutput("map-1", "number"),
                    },
                    outputs={
                        "number": FromReturnValue(),
                    },
                ),
            },
        ),
        "reduce": Task(
            sum_results,
            inputs={
                "numbers": FromNodeOutput("map", "result"),
            },
            outputs={
                "sum": FromReturnValue(),
            },
        ),
    },
)


if __name__ == "__main__":
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dag)
