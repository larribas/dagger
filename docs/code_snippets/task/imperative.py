from dagger import dsl


@dsl.task()
def hello(name):
    return f"Hello {name}!"
