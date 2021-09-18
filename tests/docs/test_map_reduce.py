import pytest

import docs.code_snippets.map_reduce.declarative as declarative
import docs.code_snippets.map_reduce.imperative as imperative
from dagger import dsl
from dagger.runtime.local import invoke


def test_declarative():
    invoke(declarative.dag)
    # Raises no exceptions


def test_imperative_can_be_built():
    dag = dsl.build(imperative.dag)
    invoke(dag)
    # Raises no exceptions


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
    assert invoke(dag) == {"return_value": b'["processed chunk1", "processed chunk2"]'}


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
        == "Error validating input 'partition' of node 'do-something-else-with': This node is partitioned by an input that comes from the output of another partitioned node. This is not a valid map-reduce pattern in dagger. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
    )

    dag = dsl.build(valid.dag)  # no error
    assert invoke(dag) == {"return_value": b'"first*$, second*$, ...*$, last*$"'}
