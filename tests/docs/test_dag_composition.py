import docs.code_snippets.dag_composition.declarative as declarative
import docs.code_snippets.dag_composition.imperative as imperative
from dagger import dsl
from dagger.runtime.local import invoke


def test_declarative():
    assert invoke(declarative.dag) == {
        "dataset": b'"original dataset, with field a encoded, with fields b and c aggregated, with moving average for d calculated"',
    }


def test_imperative_can_be_built():
    dsl.build(imperative.dag)
    # Raises no exceptions


def test_declarative_and_imperative_return_equivalent_results():
    declarative_result = invoke(declarative.dag)["dataset"]
    imperative_result = invoke(dsl.build(imperative.dag))["return_value"]

    assert declarative_result == imperative_result
