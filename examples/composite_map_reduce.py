"""
# Composite Map-Reduce.

This DAG shows how to compose map-reduce operations to model sophisticated pipelines.
"""

import dagger.dsl as dsl


@dsl.task
def generate_numbers(partitions):
    """Return a list of numbers ranging from [1, partitions]."""
    return list(range(1, partitions + 1))


@dsl.task
def map_number(n, exponent):
    """Elevate a number to an exponent."""
    return n ** exponent


@dsl.task
def sum_numbers(numbers):
    """Sum a series of numbers supplied as an Iterable."""
    return sum(numbers)


@dsl.DAG
def map_reduce(partitions, exponent):
    """Run a map-reduce operation on [1..partitions]. Mapping elevates each number to the supplied exponent. The reduction is the sum of all the elevated numbers."""
    return sum_numbers(
        [
            map_number(n=partition, exponent=exponent)
            for partition in generate_numbers(partitions)
        ]
    )


@dsl.DAG
def dag(partitions, exponent):
    """Run a map-reduce operation on [1..partitions]. Mapping calls a nested map-reduce operation with a varying number of partitions. The reduction is the sum of the result of each individual map-reduce operation."""
    return sum_numbers(
        [
            map_reduce(partitions=partition, exponent=exponent)
            for partition in generate_numbers(partitions)
        ]
    )


def run_from_cli():
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dsl.build(dag))
