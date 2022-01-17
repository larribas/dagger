from docs.code_snippets.hardcoded_values.imperative import d
from dagger import dsl
from dagger.runtime.local import invoke


def test__d__returns_4():
    dag = dsl.build(d)
    assert invoke(dag) == {"return_value": 4}
