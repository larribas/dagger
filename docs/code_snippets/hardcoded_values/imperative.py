from dagger import dsl


@dsl.task()
def sum(a, b=2):
    return a + b


@dsl.DAG()
def d(x=1):
    return sum(x, 3)
