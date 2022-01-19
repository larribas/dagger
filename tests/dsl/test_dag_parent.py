from dagger.dsl.dag_parent import DAGParent
from dagger.dsl.parameter_usage import ParameterUsage


def test__dag_parent__properties():
    inputs = {"a": ParameterUsage(name="a")}
    node_names_by_id = {"1": "x", "2": "y"}

    dag_parent = DAGParent(
        inputs=inputs,
        node_names_by_id=node_names_by_id,
    )

    assert dag_parent.inputs == inputs
    assert dag_parent.node_names_by_id == node_names_by_id


def test__dag_parent__representation():
    dag_parent = DAGParent(
        inputs={"a": ParameterUsage(name="a")},
        node_names_by_id={"1": "x", "2": "y"},
    )
    assert (
        repr(dag_parent)
        == f"DAGParent(inputs={repr(dag_parent.inputs)}, node_names_by_id={repr(dag_parent.node_names_by_id)})"
    )
