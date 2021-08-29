import dagger.dsl as dsl


@dsl.task
def generate_numbers(partitions):
    return list(range(1, partitions + 1))


@dsl.task
def map_number(n, exponent):
    return n ** exponent


@dsl.task
def sum_numbers(numbers):
    return sum(numbers)


@dsl.DAG
def map_reduce(partitions, exponent):
    return sum_numbers(
        [
            map_number(n=partition, exponent=exponent)
            for partition in generate_numbers(partitions)
        ]
    )


@dsl.DAG
def dag(partitions, exponent):
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
