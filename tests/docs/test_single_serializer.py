import docs.code_snippets.single_serializer.declarative as declarative
import docs.code_snippets.single_serializer.imperative as imperative
from dagger import dsl
from dagger.runtime.local import invoke


def test_imperative_can_be_built_and_invoked():
    @dsl.DAG()
    def dag():
        return imperative.generate_object()

    d = dsl.build(dag)
    invoke(d)
    # Raises no exceptions


def test_declarative_can_be_invoked():
    invoke(declarative.task)
    # Raises no exceptions
