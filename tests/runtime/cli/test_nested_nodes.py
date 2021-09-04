import pytest

from dagger.dag import DAG
from dagger.runtime.cli.nested_nodes import (
    NodeWithParent,
    _nodes_referenced_so_far,
    find_nested_node,
)
from dagger.task import Task

#
# find_nested_node
#


def test__find_nested_node__when_addressing_the_first_node():
    node = DAG({"a": Task(lambda: 1)})
    assert find_nested_node(node, []) == NodeWithParent(node=node)


def test__find_nested_node__when_addressing_a_task_within_a_dag():
    task = Task(lambda: 1)
    dag = DAG({"a": task})
    assert find_nested_node(dag, ["a"]) == NodeWithParent(
        node=task,
        node_name="a",
        parent=NodeWithParent(
            node=dag,
            node_name="",
        ),
    )


def test__find_nested_node__when_addressing_a_nested_task():
    task = Task(lambda: 1)
    inner_dag = DAG({"a": task})
    outer_dag = DAG({"b": inner_dag})
    assert find_nested_node(outer_dag, ["b", "a"]) == NodeWithParent(
        node=task,
        node_name="a",
        parent=NodeWithParent(
            node=inner_dag,
            node_name="b",
            parent=NodeWithParent(
                node=outer_dag,
            ),
        ),
    )


def test__find_nested_node__when_addressing_a_nested_dag():
    task = Task(lambda: 1)
    inner_dag = DAG({"a": task})
    outer_dag = DAG({"b": inner_dag})
    assert find_nested_node(outer_dag, ["b"]) == NodeWithParent(
        node=inner_dag,
        node_name="b",
        parent=NodeWithParent(node=outer_dag),
    )


def test__find_nested_node__when_address_is_too_deep():
    task = Task(lambda: 1)
    inner_dag = DAG({"a": task})
    outer_dag = DAG({"b": inner_dag})

    with pytest.raises(ValueError) as e:
        find_nested_node(outer_dag, ["b", "a", "too", "deep"])

    assert (
        str(e.value)
        == "Node 'b.a' does not contain any other nodes. However, you are trying to access a subnode 'too.deep'."
    )


def test__find_nested_node__when_address_is_wrong():
    task = Task(lambda: 1)
    inner_dag = DAG({"z": task, "a": task})
    outer_dag = DAG({"b": inner_dag})

    with pytest.raises(ValueError) as e:
        find_nested_node(outer_dag, ["b", "x"])

    assert (
        str(e.value)
        == "You selected node 'b.x'. However, DAG 'b' does not contain any node with such a name. These are the names the DAG contains: ['a', 'z']"
    )


#
# nodes_referenced_so_far
#


def test__nodes_referenced_so_far__when_none():
    assert _nodes_referenced_so_far() == []


def test__nodes_referenced_so_far__one_level_of_nesting():
    assert (
        _nodes_referenced_so_far(
            node=NodeWithParent(
                node=Task(lambda: 1),
                node_name="x",
            )
        )
        == ["x"]
    )


def test__nodes_referenced_so_far__several_levels_of_nesting():
    task = Task(lambda: 1)
    inner_dag = DAG({"a": task})
    outer_dag = DAG({"b": inner_dag})

    assert (
        _nodes_referenced_so_far(
            node=NodeWithParent(
                node=task,
                node_name="a",
                parent=NodeWithParent(
                    node=inner_dag,
                    node_name="b",
                    parent=NodeWithParent(
                        node=outer_dag,
                    ),
                ),
            )
        )
        == ["b", "a"]
    )


def test__node_with_parent__representation():
    task = Task(lambda: 1)
    dag = DAG({"a": task})
    node_with_parent = find_nested_node(dag, ["a"])
    assert (
        repr(node_with_parent)
        == f"NodeWithParent(node={repr(task)}, node_name=a, parent=NodeWithParent(node={repr(dag)}, node_name=, parent=None))"
    )
