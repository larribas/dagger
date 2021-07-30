"""Verify the equivalence of DAGs built using the DSL vs. the data structures."""

from dagger.dag import DAG


def verify_dags_are_equivalent(
    built_by_dsl: DAG,
    built_declaratively: DAG,
):
    """Verify that two DAGs are equivalent in terms of structure and behavior."""
    # Smaller asserts to pinpoint where an issue may be specifically
    assert (
        built_by_dsl.inputs.keys() == built_declaratively.inputs.keys()
    ), f"Built input list was {list(built_by_dsl.inputs.keys())}, but expected input list was {list(built_declaratively.inputs.keys())}"
    for input_name in built_by_dsl.inputs:
        assert (
            built_by_dsl.inputs[input_name] == built_declaratively.inputs[input_name]
        ), f"input '{input_name}' is different"

    assert (
        built_by_dsl.outputs.keys() == built_declaratively.outputs.keys()
    ), f"Built output list was {list(built_by_dsl.outputs.keys())}, but expected output list was {list(built_declaratively.outputs.keys())}"
    for output_name in built_by_dsl.outputs:
        assert (
            built_by_dsl.outputs[output_name]
            == built_declaratively.outputs[output_name]
        ), f"output '{output_name}' is different"

    assert (
        built_by_dsl.nodes.keys() == built_declaratively.nodes.keys()
    ), f"Built node list was {list(built_by_dsl.nodes.keys())}, but expected node list was {list(built_declaratively.nodes.keys())}"
    for node_name in built_by_dsl.nodes:
        assert (
            built_by_dsl.nodes[node_name] == built_declaratively.nodes[node_name]
        ), f"""Node '{node_name}' is different. 

        We expected:
        {built_declaratively.nodes[node_name]}

        Instead, it was:
        {built_by_dsl.nodes[node_name]}
        """

    assert built_by_dsl.runtime_options == built_declaratively.runtime_options

    # Make sure they are equivalent following the exact semantics defined by the DAG data structure
    assert built_by_dsl == built_declaratively
