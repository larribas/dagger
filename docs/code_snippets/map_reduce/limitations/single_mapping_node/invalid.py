from typing import List

from dagger import dsl

Partition = str


@dsl.task()
def get_partitions() -> List[Partition]:
    return ["first", "second", "...", "last"]


@dsl.task()
def do_something_with(partition: Partition) -> Partition:
    return f"{partition}*"


@dsl.task()
def do_something_else_with(partition: Partition) -> Partition:
    return f"{partition}$"


@dsl.task()
def aggregate(partial_results: List[Partition]):
    return ", ".join(partial_results)


@dsl.DAG()
def dag():
    partitions = get_partitions()
    partial_results = []
    for partition in partitions:
        transformed_partition = do_something_with(partition)
        partial_results.append(
            do_something_else_with(transformed_partition),
        )

    return aggregate(partial_results)
