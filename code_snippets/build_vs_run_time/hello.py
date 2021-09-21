from dagger import dsl
from dagger.runtime.local import invoke


@dsl.task()
def my_task():
    print("Hello at runtime!")


@dsl.DAG()
def my_pipeline():
    print("Hello at build time!")
    my_task()


dag = dsl.build(my_pipeline)
invoke(dag)
