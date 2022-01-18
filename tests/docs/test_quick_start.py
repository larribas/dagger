from dagger import dsl
from dagger.runtime.local import invoke
from docs.code_snippets.quick_start.build_dag import dag
from docs.code_snippets.quick_start.invoke_dag import result
from docs.code_snippets.quick_start.invoke_dag_with_defaults import (
    result as result_defaults,
)
from docs.code_snippets.quick_start.quick_start import map_reduce_pipeline


def test_build():
    dsl.build(map_reduce_pipeline)
    # Raises no exceptions


def test_invoke_dag():
    res = invoke(dag, params={"seed": 1, "exponent": 2})
    assert res == {"return_value": 91}


def test_invoke():
    assert result == {"return_value": 91}


def test_invoke_with_default():
    assert result_defaults == {"return_value": 91}
