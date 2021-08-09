"""
# Hello World.

Simplest possible workflow. We define a DAG with a single task: to print "Hello world".
"""
from dagger import dsl


@dsl.task
def say_hello_world():  # noqa
    print("Hello world")


@dsl.DAG
def dag():  # noqa
    say_hello_world()


def run_from_cli():
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dag)
