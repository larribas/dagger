"""
# Hello World.

Simplest possible workflow. We define a DAG with a single task: to print "Hello world".
"""
from dagger import DAG, Task


def say_hello_world():  # noqa
    print("Hello world")


dag = DAG(
    nodes={
        "say-hello-world": Task(say_hello_world),
    },
)


if __name__ == "__main__":
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dag)
