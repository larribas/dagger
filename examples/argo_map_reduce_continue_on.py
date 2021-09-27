"""
# Map-Reduce continues on failure in Argo.

This DAG executes a map-reduce operation where some of the "mapping" operations fail, but the DAG runs to completion and is able to run the "reduce" step only with the successful results.
"""

from dagger import dsl


@dsl.task()
def generate_partitions():  # noqa
    return [1, 2, 3, 4]


@dsl.task(
    runtime_options={
        "argo_task_overrides": {
            "continueOn": {
                "failed": True,
            },
        },
    }
)
def succeed_if_even(num):  # noqa
    if (num % 2) == 0:
        return num
    else:
        raise ValueError("This is odd!")


@dsl.task()
def gather_all_even_numbers(numbers):  # noqa
    print(f"Even numbers were: {numbers}")


@dsl.DAG()
def dag():  # noqa
    even_numbers = [succeed_if_even(num) for num in generate_partitions()]
    gather_all_even_numbers(even_numbers)


if __name__ == "__main__":
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dsl.build(dag))
