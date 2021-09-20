from dagger import dsl
from dagger.runtime.local import invoke


def test_hello():
    from docs.code_snippets.build_vs_run_time.hello import my_pipeline

    dag = dsl.build(my_pipeline)
    invoke(dag)
    # Raises no errors


def test_mixing_decorated_and_undecorated_functions():
    from docs.code_snippets.build_vs_run_time.mixing_decorated_and_undecorated_functions import (
        my_pipeline,
    )

    dag = dsl.build(my_pipeline)
    invoke(dag)
    # Raises no errors
