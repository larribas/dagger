from dagger import dsl


@dsl.task()
def echo(message):
    print(message)


@dsl.DAG()
def my_pipeline(message1, message2):
    echo(message1)
    echo(message2)


from dagger.runtime.local import invoke

# When using the DSL, remember to ALWAYS call dsl.build()
dag = dsl.build(my_pipeline)

invoke(
    dag,
    params={
        "message1": "Hello",
        "message2": "World",
    },
)
