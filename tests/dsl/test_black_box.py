"""
Black-box test suite for the imperative DSL.

This module tests increasingly complex DAG definitions using the imperative DSL,
from the standpoint of a user of the library. That is, without any explicit
knowledge about the internal data structures that build the DAG under the hood.
"""


import dagger.dsl as dsl
from dagger.dag import DAG
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromKey, FromReturnValue
from dagger.task import Task
from tests.dsl.verification import verify_dags_are_equivalent


def test__hello_world():
    @dsl.task
    def say_hello_world():
        print("hello world")

    @dsl.DAG
    def dag():
        say_hello_world()

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            nodes={
                "say-hello-world": Task(say_hello_world.func),
            }
        ),
    )


def test__multiple_calls_to_the_same_task():
    @dsl.task
    def f():
        pass

    @dsl.DAG
    def dag():
        f()
        f()
        f()

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            nodes={
                "f-1": Task(f.func),
                "f-2": Task(f.func),
                "f-3": Task(f.func),
            }
        ),
    )


def test__input_from_param():
    @dsl.task
    def say_hello(first_name):
        return f"hello {first_name}"

    @dsl.DAG
    def dag(first_name):
        say_hello(first_name)

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            inputs={
                "first_name": FromParam("first_name"),
            },
            nodes={
                "say-hello": Task(
                    say_hello.func,
                    inputs={"first_name": FromParam("first_name")},
                ),
            },
        ),
    )


def test__input_from_param_with_different_names():
    @dsl.task
    def say_hello(first_name):
        return f"hello {first_name}"

    @dsl.DAG
    def dag(name):
        say_hello(name)

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            inputs={
                "name": FromParam("name"),
            },
            nodes={
                "say-hello": Task(
                    say_hello.func,
                    inputs={"first_name": FromParam("name")},
                ),
            },
        ),
    )


def test__input_from_node_output():
    @dsl.task
    def generate_random_number():
        import random

        return random.random()

    @dsl.task
    def announce_number(n):
        print(f"the number was {n}!")

    @dsl.DAG
    def dag():
        number = generate_random_number()
        announce_number(number)

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            nodes={
                "generate-random-number": Task(
                    generate_random_number.func,
                    outputs={"return_value": FromReturnValue()},
                ),
                "announce-number": Task(
                    announce_number.func,
                    inputs={
                        "n": FromNodeOutput("generate-random-number", "return_value")
                    },
                ),
            },
        ),
    )


def test__using_sub_outputs():
    @dsl.task
    def generate_complex_structure() -> dict:
        return {"a": 1, "b": 2}

    @dsl.task
    def print_a_and_b(a, b):
        print(f"a={a}, b={b}")

    @dsl.DAG
    def dag():
        d = generate_complex_structure()
        print_a_and_b(d["a"], d["b"])

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            nodes={
                "generate-complex-structure": Task(
                    generate_complex_structure.func,
                    outputs={
                        "key_a": FromKey("a"),
                        "key_b": FromKey("b"),
                    },
                ),
                "print-a-and-b": Task(
                    print_a_and_b.func,
                    inputs={
                        "a": FromNodeOutput("generate-complex-structure", "key_a"),
                        "b": FromNodeOutput("generate-complex-structure", "key_b"),
                    },
                ),
            },
        ),
    )


def test__dag_outputs_from_return_value():
    @dsl.task
    def generate_complex_structure() -> dict:
        return {"a": 1, "b": 2}

    @dsl.DAG
    def dag():
        return generate_complex_structure()

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            nodes={
                "generate-complex-structure": Task(
                    generate_complex_structure.func,
                    outputs={"return_value": FromReturnValue()},
                ),
            },
            outputs={
                "return_value": FromNodeOutput(
                    "generate-complex-structure", "return_value"
                )
            },
        ),
    )


def test__dag_outputs_from_sub_output():
    @dsl.task
    def generate_complex_structure() -> dict:
        return {"a": 1, "b": 2}

    @dsl.DAG
    def dag():
        return generate_complex_structure()["a"]

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            nodes={
                "generate-complex-structure": Task(
                    generate_complex_structure.func,
                    outputs={"key_a": FromKey("a")},
                ),
            },
            outputs={
                "return_value": FromNodeOutput("generate-complex-structure", "key_a")
            },
        ),
    )


def test__multiple_dag_outputs():
    @dsl.task
    def generate_number() -> int:
        return 1

    @dsl.DAG
    def dag():
        return {
            "a": generate_number(),
            "b": generate_number(),
        }

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            nodes={
                "generate-number-1": Task(
                    generate_number.func,
                    outputs={"return_value": FromReturnValue()},
                ),
                "generate-number-2": Task(
                    generate_number.func,
                    outputs={"return_value": FromReturnValue()},
                ),
            },
            outputs={
                "a": FromNodeOutput("generate-number-1", "return_value"),
                "b": FromNodeOutput("generate-number-2", "return_value"),
            },
        ),
    )


def test__nested_dags_simple():
    @dsl.task
    def double(n: int) -> int:
        return n * 2

    @dsl.DAG
    def inner_dag(n: int) -> int:
        return double(n)

    @dsl.DAG
    def outer_dag(n: int):
        return inner_dag(n)

    verify_dags_are_equivalent(
        dsl.build(outer_dag),
        DAG(
            inputs={"n": FromParam("n")},
            outputs={"return_value": FromNodeOutput("inner-dag", "return_value")},
            nodes={
                "inner-dag": DAG(
                    inputs={"n": FromParam("n")},
                    outputs={"return_value": FromNodeOutput("double", "return_value")},
                    nodes={
                        "double": Task(
                            double.func,
                            inputs={"n": FromParam("n")},
                            outputs={"return_value": FromReturnValue()},
                        ),
                    },
                ),
            },
        ),
    )


def test__nested_dags_complex():
    @dsl.task
    def generate_seed() -> int:
        return 100

    @dsl.task
    def generate_random_number(seed: int) -> int:
        import random

        random.seed(seed)
        return random.randint(0, 1000)

    @dsl.task
    def multiply_number(number: int, multiplier: int) -> int:
        return number * multiplier

    @dsl.task
    def print_number(number: int):
        print(f"I was told to print number {number}")

    @dsl.DAG
    def inner_dag(seed: int, multiplier: int) -> int:
        number = generate_random_number(seed)
        return multiply_number(number, multiplier)

    @dsl.DAG
    def outer_dag(multiplier: int):
        seed = generate_seed()
        multiplied_number = inner_dag(seed=seed, multiplier=multiplier)
        print_number(multiplied_number)

    verify_dags_are_equivalent(
        dsl.build(outer_dag),
        DAG(
            inputs={
                "multiplier": FromParam("multiplier"),
            },
            nodes={
                "generate-seed": Task(
                    generate_seed.func,
                    outputs={"return_value": FromReturnValue()},
                ),
                "inner-dag": DAG(
                    inputs={
                        "multiplier": FromParam("multiplier"),
                        "seed": FromNodeOutput("generate-seed", "return_value"),
                    },
                    outputs={
                        "return_value": FromNodeOutput(
                            "multiply-number", "return_value"
                        ),
                    },
                    nodes={
                        "generate-random-number": Task(
                            generate_random_number.func,
                            inputs={"seed": FromParam("seed")},
                            outputs={"return_value": FromReturnValue()},
                        ),
                        "multiply-number": Task(
                            multiply_number.func,
                            inputs={
                                "multiplier": FromParam("multiplier"),
                                "number": FromNodeOutput(
                                    "generate-random-number", "return_value"
                                ),
                            },
                            outputs={"return_value": FromReturnValue()},
                        ),
                    },
                ),
                "print-number": Task(
                    print_number.func,
                    inputs={"number": FromNodeOutput("inner-dag", "return_value")},
                ),
            },
        ),
    )


def test__runtime_options():
    task_options = {"task_options": 1}
    dag_options = {"dag_options": 2}

    @dsl.task
    def say_hello_world():
        print("hello world")

    @dsl.DAG
    def inner_dag():
        say_hello_world.with_runtime_options(task_options)()

    @dsl.DAG
    def outer_dag():
        inner_dag.with_runtime_options(dag_options)()

    verify_dags_are_equivalent(
        dsl.build(outer_dag),
        DAG(
            nodes={
                "inner-dag": DAG(
                    nodes={
                        "say-hello-world": Task(
                            say_hello_world.func,
                            runtime_options=task_options,
                        ),
                    },
                    runtime_options=dag_options,
                ),
            },
        ),
    )
