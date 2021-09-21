from dagger import dsl
from dagger.runtime.local import invoke


def test_imperative():
    from docs.code_snippets.cli_runtime.imperative import my_pipeline

    dag = dsl.build(my_pipeline)
    invoke(dag, params={"name": "Mr. Bond"})
    # Raises no exceptions


def test_declarative():
    from docs.code_snippets.cli_runtime.declarative import dag

    invoke(dag, params={"name": "Mr. Bond"})
    # Raises no exceptions (the code snippet itself also builds the DAG and runs it)
