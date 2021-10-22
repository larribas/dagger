"""
# Map-Reduce Imperative.

This DAG shows how to create a dynamic fan-out and fan-in operation with an imperative syntax.

The DAG has an input parameter called exponent.

There three tasks:

* generate_numbers: generate a list sequential integer number from 0 to n, where n is a randomly generated integer from 3 to 20.

* raise_number: fan-out operation that raise each number in the list to exponent.

* sum_numbers: fan-in operation that sums all raise numbers and return the result.

"""

import random

from dagger import dsl


@dsl.task()
def generate_numbers():
    """Generates a list sequential integer number from 0 to n, where n is a randomly
    generated integer from 3 to 20."""
    numbers = list(range(random.randint(3, 20)))
    print(f"Generating the following list of numbers: {numbers}")
    return numbers


@dsl.task()
def raise_number(n, exponent):
    """ Fan-out operation that raise each number in the list to exponent. """
    print(f"Raising {n} to a power of {exponent}")
    return n ** exponent


@dsl.task()
def sum_numbers(numbers):
    """  Fan-in operation that sums all raise numbers and return the result. """
    print(f"Calculating the sum of {numbers}")
    return sum(numbers)


@dsl.DAG()
def map_reduce_pipeline(exponent):
    """Define the DAG, i.e. the dependencies among the tasks, in particular the
    fan-out fan-in operation."""
    print("\n\n Generating the DAG Data Structures")
    print("-" * 36)
    return sum_numbers([raise_number(n, exponent) for n in generate_numbers()])


if __name__ == "__main__":
    """Define a local interface for this DAG, using the local runtime. Check the
    documentation to understand why this is relevant or necessary."""
    from dagger.runtime.local import invoke

    dag = dsl.build(map_reduce_pipeline)
    # print(dag)
    print(invoke(dag, {"exponent": 2}))
