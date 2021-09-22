from dagger import dsl
from dagger.runtime.local import invoke
from docs.code_snippets.quick_start import map_reduce_pipeline


def test_build():
    dsl.build(map_reduce_pipeline)
    # Raises no exceptions


def test_invoke():
    dag = dsl.build(map_reduce_pipeline)
    result = invoke(dag, params={"seed": 1, "exponent": 2})
    assert result == {"return_value": 91}
