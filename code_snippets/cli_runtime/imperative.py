from dagger import dsl


@dsl.task()
def say_hello(name):
    print(f"Hello {name}!")


@dsl.DAG()
def my_pipeline(name):
    say_hello(name)


if __name__ == "__main__":
    from dagger.runtime.cli import invoke

    # When using the DSL, remember to ALWAYS call dsl.build()
    dag = dsl.build(my_pipeline)

    invoke(dag)
