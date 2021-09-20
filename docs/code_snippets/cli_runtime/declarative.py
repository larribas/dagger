from dagger import DAG, FromParam, Task


def say_hello(name):
    print(f"Hello {name}!")


dag = DAG(
    inputs={
        "name": FromParam(),
    },
    nodes={
        "say-hello": Task(
            say_hello,
            inputs={
                "name": FromParam("name"),
            },
        ),
    },
)


if __name__ == "__main__":
    from dagger.runtime.cli import invoke

    invoke(dag)
