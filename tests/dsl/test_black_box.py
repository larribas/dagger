"""
Black-box test suite for the imperative DSL.

This module tests increasingly complex DAG definitions using the imperative DSL,
from the standpoint of a user of the library. That is, without any explicit
knowledge about the internal data structures that build the DAG under the hood.
"""

import random
from typing import Annotated, Mapping

import dagger.dsl as dsl
from dagger.dag import DAG
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromKey, FromReturnValue
from dagger.serializer import AsJSON, AsPickle
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


def test__input_from_literal_value():
    class ArbitraryObject:
        pass

    literal_values = [
        "string literal",
        2,
        5.5,
        True,
        ["a", "list"],
        {"complex": ArbitraryObject()},
    ]

    @dsl.task
    def inspect(hello, value):
        return f"{hello} {value} of type {type(value).__name__}"

    @dsl.DAG
    def dag(hello):
        for value in literal_values:
            inspect(hello="hello...", value=value)

    for i, value in enumerate(literal_values):
        assert (
            dsl.build(dag).nodes[f"inspect-{i + 1}"].func()
            == f"hello... {value} of type {type(value).__name__}"
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


def test__overriding_serializers():
    @dsl.task
    def generate_single_number() -> Annotated[int, dsl.Serialize(AsPickle())]:
        return random.randint(1, 100)

    @dsl.task
    def generate_multiple_numbers() -> Annotated[
        Mapping[str, int],
        dsl.Serialize(json=AsJSON(indent=5), pickle=AsPickle()),
    ]:
        return {
            "json": 2,
            "pickle": 3,
        }

    @dsl.task
    def announce_number(n: int):
        print(f"the number was {n}")

    @dsl.DAG
    def announce_numbers(param: int, rand: int, json: int, pickle: int):
        announce_number(param)
        announce_number(rand)
        announce_number(json)
        announce_number(pickle)

    @dsl.DAG
    def dag(param: int):
        rand = generate_single_number()
        mult = generate_multiple_numbers()
        announce_numbers(
            param=param,
            rand=rand,
            json=mult["json"],
            pickle=mult["pickle"],
        )

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            inputs={"param": FromParam("param")},
            nodes={
                "generate-single-number": Task(
                    generate_single_number.func,
                    outputs={"return_value": FromReturnValue(serializer=AsPickle())},
                ),
                "generate-multiple-numbers": Task(
                    generate_multiple_numbers.func,
                    outputs={
                        "key_json": FromKey(key="json", serializer=AsJSON(indent=5)),
                        "key_pickle": FromKey(key="pickle", serializer=AsPickle()),
                    },
                ),
                "announce-numbers": DAG(
                    inputs={
                        "param": FromParam("param"),
                        "rand": FromNodeOutput(
                            "generate-single-number",
                            "return_value",
                            serializer=AsPickle(),
                        ),
                        "json": FromNodeOutput(
                            "generate-multiple-numbers",
                            "key_json",
                            serializer=AsJSON(indent=5),
                        ),
                        "pickle": FromNodeOutput(
                            "generate-multiple-numbers",
                            "key_pickle",
                            serializer=AsPickle(),
                        ),
                    },
                    nodes={
                        "announce-number-1": Task(
                            announce_number.func,
                            inputs={
                                "n": FromParam("param"),
                            },
                        ),
                        "announce-number-2": Task(
                            announce_number.func,
                            inputs={
                                "n": FromParam("rand", serializer=AsPickle()),
                            },
                        ),
                        "announce-number-3": Task(
                            announce_number.func,
                            inputs={
                                "n": FromParam("json", serializer=AsJSON(indent=5)),
                            },
                        ),
                        "announce-number-4": Task(
                            announce_number.func,
                            inputs={
                                "n": FromParam("pickle", serializer=AsPickle()),
                            },
                        ),
                    },
                ),
            },
        ),
    )


def test__map_reduce():
    @dsl.task
    def generate_numbers():
        return [1, 2, 3]

    @dsl.task
    def map_number(n, exponent):
        return n ** exponent

    @dsl.task
    def sum_numbers(numbers):
        return sum(numbers)

    @dsl.DAG
    def dag(exponent):
        partitions = generate_numbers()

        numbers = [
            map_number(n=partition, exponent=exponent) for partition in partitions
        ]

        return sum_numbers(numbers)

    verify_dags_are_equivalent(
        dsl.build(dag),
        DAG(
            inputs={
                "exponent": FromParam("exponent"),
            },
            outputs={
                "return_value": FromNodeOutput("sum-numbers", "return_value"),
            },
            nodes={
                "generate-numbers": Task(
                    generate_numbers.func,
                    outputs={
                        "return_value": FromReturnValue(is_partitioned=True),
                    },
                ),
                "map-number": Task(
                    map_number.func,
                    inputs={
                        "n": FromNodeOutput("generate-numbers", "return_value"),
                        "exponent": FromParam("exponent"),
                    },
                    outputs={
                        "return_value": FromReturnValue(),
                    },
                    partition_by_input="n",
                ),
                "sum-numbers": Task(
                    sum_numbers.func,
                    inputs={
                        "numbers": FromNodeOutput("map-number", "return_value"),
                    },
                    outputs={
                        "return_value": FromReturnValue(),
                    },
                ),
            },
        ),
    )
