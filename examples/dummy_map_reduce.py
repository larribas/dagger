import random

from dagger import dsl


@dsl.task()
def generate_numbers():
    numbers = list(range(random.randint(3, 20)))
    print(f"Generating the following list of numbers: {numbers}")
    return numbers


@dsl.task()
def raise_number(n, exponent):
    print(f"Raising {n} to a power of {exponent}")
    return n ** exponent


@dsl.task()
def sum_numbers(numbers):
    print(f"Calculating the sum of {numbers}")
    return sum(numbers)


@dsl.DAG()
def map_reduce_pipeline(exponent):
    print("\n\n Generating the DAG Data Structures")
    print("-" * 36)
    return sum_numbers([raise_number(n, exponent) for n in generate_numbers()])


if __name__ == "__main__":
    from dagger.runtime.local import invoke

    dag = dsl.build(map_reduce_pipeline)
    # print(dag)
    print(invoke(dag, {"exponent": 2}))
