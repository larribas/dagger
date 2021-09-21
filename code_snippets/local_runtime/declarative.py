from dagger import DAG, FromParam, Task


def echo(message):
    print(message)


dag = DAG(
    inputs={
        "message1": FromParam(),
        "message2": FromParam(),
    },
    nodes={
        "echo-message-1": Task(
            echo,
            inputs={
                "message": FromParam("message1"),
            },
        ),
        "echo-message-2": Task(
            echo,
            inputs={
                "message": FromParam("message2"),
            },
        ),
    },
)


from dagger.runtime.local import invoke

invoke(
    dag,
    params={
        "message1": "Hello",
        "message2": "World",
    },
)
