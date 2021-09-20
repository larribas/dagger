import docs.code_snippets.multiple_serializers.declarative as declarative
import docs.code_snippets.multiple_serializers.imperative as imperative
from dagger import dsl
from dagger.runtime.local import invoke


def test_imperative_can_be_built_and_invoked():
    @dsl.DAG()
    def dag():
        output = imperative.generate_multiple_outputs()
        return {
            "bool": output["a_boolean"],
            "custom_obj": output["a_custom_object"],
        }

    d = dsl.build(dag)
    invoke(d)
    # Raises no exceptions


def test_declarative_can_be_invoked():
    invoke(declarative.task)
    # Raises no exceptions
