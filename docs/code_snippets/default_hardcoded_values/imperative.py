from dagger import dsl


@dsl.task()
def add(x, y, z):
    return f"{x}-{y}-{z}"


@dsl.DAG()
def nested_d(a, b=2, c=3):
    return add(a, b, c)


@dsl.DAG()
def d(a=10):
    return nested_d(a, c=20)
