import random

from dagger import dsl
from dagger.runtime.local import invoke


@dsl.task()
def generate_numbers(seed):
    random.seed(seed)
    length = random.randint(3, 20)
    numbers = list(range(length))
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
def map_reduce_pipeline(seed, exponent=2):
    numbers = generate_numbers(seed)

    raised_numbers = []
    for n in numbers:
        raised_numbers.append(raise_number(n, exponent))

    return sum_numbers(raised_numbers)
