from dagger import FromParam, FromReturnValue, Task


def hello(name):
    return f"Hello {name}!"


task = Task(
    hello,
    inputs={
        "name": FromParam(),
    },
    outputs={
        "hello_message": FromReturnValue(),
    },
)
