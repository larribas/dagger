import dagger.dsl as dsl
import dagger.input as input
import dagger.output as output
from dagger.dag import DAG
from dagger.task import Task
from tests.dsl.verification import verify_dags_are_equivalent


def test__contexts_in_each_dag_are_isolated():
    @dsl.task
    def f():
        return 1

    @dsl.DAG
    def build_dag_1():
        f()

    @dsl.DAG
    def build_dag_2():
        f()

    # If the context was shared between dag_1 and dag_2, the second call would accumulate
    # the results of the previous one and it would yield a different result.
    assert build_dag_1() == build_dag_2()


def test__hello_world():
    @dsl.task
    def say_hello_world():
        print("hello world")

    @dsl.DAG
    def build_dag():
        say_hello_world()

    verify_dags_are_equivalent(
        build_dag(),
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
    def build_dag():
        f()
        f()
        f()

    verify_dags_are_equivalent(
        build_dag(),
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
    def build_dag(first_name):
        say_hello(first_name)

    verify_dags_are_equivalent(
        build_dag(),
        DAG(
            inputs={
                "first_name": input.FromParam(),
            },
            nodes={
                "say-hello": Task(
                    say_hello.func,
                    inputs=dict(first_name=input.FromParam()),
                ),
            },
        ),
    )


def test__input_from_param_with_different_names():
    @dsl.task
    def say_hello(first_name):
        return f"hello {first_name}"

    @dsl.DAG
    def build_dag(name):
        say_hello(name)

    verify_dags_are_equivalent(
        build_dag(),
        DAG(
            inputs={
                "name": input.FromParam(),
            },
            nodes={
                "say-hello": Task(
                    say_hello.func,
                    inputs=dict(first_name=input.FromParam("name")),
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
    def build_dag():
        number = generate_random_number()
        announce_number(number)

    verify_dags_are_equivalent(
        build_dag(),
        DAG(
            nodes={
                "generate-random-number": Task(
                    generate_random_number.func,
                    outputs=dict(output=output.FromReturnValue()),
                ),
                "announce-number": Task(
                    announce_number.func,
                    inputs=dict(
                        n=input.FromNodeOutput("generate-random-number", "output"),
                    ),
                ),
            },
        ),
    )
