import pytest

import docs.code_snippets.map_reduce.declarative as declarative
import docs.code_snippets.map_reduce.imperative as imperative
from dagger import dsl
from dagger.runtime.local import invoke


def test_imperative_can_be_built():
    dsl.build(imperative.dag)
    # Raises no exceptions


def test_declarative_and_imperative_return_equivalent_results():
    declarative_result = invoke(declarative.dag)["best_model"]
    imperative_result = invoke(dsl.build(imperative.dag))["return_value"]

    assert declarative_result == imperative_result


def test_dag_output_from_partitioned_node():
    import docs.code_snippets.map_reduce.limitations.dag_output_from_partitioned_node.invalid as invalid
    import docs.code_snippets.map_reduce.limitations.dag_output_from_partitioned_node.valid as valid

    with pytest.raises(TypeError) as e:
        dsl.build(invalid.dag)

    assert (
        str(e.value)
        == "This DAG returned a value of type list. Functions decorated with `dsl.DAG` may only return two types of values: The output of another node or a mapping of [str, the output of another node]"
    )

    dag = dsl.build(valid.dag)  # no error
    assert invoke(dag) == {"return_value": ["processed chunk1", "processed chunk2"]}


def test_for_based_on_param():
    import docs.code_snippets.map_reduce.limitations.for_based_on_param.invalid as invalid
    import docs.code_snippets.map_reduce.limitations.for_based_on_param.valid as valid

    with pytest.raises(ValueError) as e:
        dsl.build(invalid.dag)

    assert (
        str(e.value)
        == "Iterating over the value of a parameter is not a valid parallelization pattern in Dagger. You need to convert the parameter into the output of a node. Read this section in the documentation to find out more: https://larribas.me/dagger/user-guide/dags/map-reduce"
    )

    invoke(
        dsl.build(valid.dag),
        params={"countries": ["es", "us"]},
    )  # no error


def test_nested_for_loops():
    import docs.code_snippets.map_reduce.limitations.nested_for_loops.invalid as invalid
    import docs.code_snippets.map_reduce.limitations.nested_for_loops.valid as valid

    with pytest.raises(ValueError) as e:
        dsl.build(invalid.dag)

    assert (
        str(e.value)
        == "The following inputs to this node are partitioned: ['product_category', 'user_cohort']. However, nodes may only be partitioned by one of their inputs. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
    )

    invoke(dsl.build(valid.dag))  # no error


def test_single_mapping_node():
    import docs.code_snippets.map_reduce.limitations.single_mapping_node.invalid as invalid
    import docs.code_snippets.map_reduce.limitations.single_mapping_node.valid as valid

    with pytest.raises(ValueError) as e:
        dsl.build(invalid.dag)

    assert (
        str(e.value)
        == "Node 'do-something-else-with' is partitioned by an input that comes from the output of another node, 'do-something-with'. Node 'do-something-with' is also partitioned. In Dagger, a node cannot be partitioned by the output of another partitioned node. Check the documentation to better understand how partitioning works: https://larribas.me/dagger/user-guide/partitioning/"
    )

    dag = dsl.build(valid.dag)  # no error
    assert invoke(dag) == {"return_value": "first*$, second*$, ...*$, last*$"}
