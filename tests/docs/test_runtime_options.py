import docs.code_snippets.runtime_options.declarative as declarative
import docs.code_snippets.runtime_options.imperative as imperative
from dagger import dsl
from dagger.runtime.local import invoke


def test_imperative_can_be_built():
    dsl.build(imperative.dag)
    # Raises no exceptions


def test_declarative_and_imperative_return_equivalent_results():
    params = {"query": "SELECT * FROM my_table"}
    declarative_result = invoke(declarative.dag, params=params)["results"]
    imperative_result = invoke(
        dsl.build(imperative.dag),
        params=params,
    )["return_value"]

    assert declarative_result == imperative_result
