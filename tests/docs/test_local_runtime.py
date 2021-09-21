from dagger import dsl
from dagger.runtime.local import invoke


def test_imperative():
    from docs.code_snippets.local_runtime.imperative import my_pipeline

    dsl.build(my_pipeline)
    # Raises no exceptions (the code snippet itself also builds the DAG and runs it)


def test_declarative():
    from docs.code_snippets.local_runtime.declarative import dag

    invoke(dag, params={"message1": "a", "message2": "b"})
    # Raises no exceptions (the code snippet itself also builds the DAG and runs it)
