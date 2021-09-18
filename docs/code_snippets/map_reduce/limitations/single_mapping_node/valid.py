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


@dsl.DAG()
def map_partition(partition: Partition):
    transformed_partition = do_something_with(partition)
    return do_something_else_with(transformed_partition)


@dsl.task()
def aggregate(partial_results: List[Partition]):
    return ", ".join(partial_results)


@dsl.DAG()
def dag():
    partitions = get_partitions()
    partial_results = [map_partition(partition) for partition in partitions]
    return aggregate(partial_results)

    # or simply:
    # return aggregate([map_partition(partition) for partition in get_partitions()])
