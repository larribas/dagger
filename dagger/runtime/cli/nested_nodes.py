"""Select a node in a nested DAG structure for execution."""


from typing import List, Optional

from dagger.dag import Node
from dagger.task import Task


class NodeWithParent:
    """Reference to a Node in a deeply-nested DAG structure."""

    def __init__(
        self,
        node: Node,
        node_name: str = "",
        parent: Optional["NodeWithParent"] = None,
    ):
        self._node = node
        self._node_name = node_name
        self._parent = parent

    @property
    def node(self) -> Node:
        """Return the current node."""
        return self._node

    @property
    def node_name(self) -> str:
        """Return the name of the current node with respect to its parent."""
        return self._node_name

    @property
    def parent(self) -> Optional["NodeWithParent"]:
        """Return the parent of the current node."""
        return self._parent

    def __repr__(self) -> str:
        """Represent an instance of this class in a human-readable format."""
        return f"NodeWithParent(node={self._node}, node_name={self._node_name}, parent={self._parent})"

    def __eq__(self, obj) -> bool:
        """Return true if obj is equivalent to self."""
        return (
            isinstance(obj, NodeWithParent)
            and self._node == obj._node
            and self._node_name == obj._node_name
            and self._parent == obj._parent
        )


def _nodes_referenced_so_far(node: Optional[NodeWithParent] = None) -> List[str]:
    if not node:
        return []

    if node.node_name == "":
        return _nodes_referenced_so_far(node.parent)

    return _nodes_referenced_so_far(node.parent) + [node.node_name]


def find_nested_node(
    node: Node,
    address: List[str],
    node_name: str = "",
    parent: Optional[NodeWithParent] = None,
) -> NodeWithParent:
    """
    Return a nested node and its parents.

    The function is designed as a recursive function.

    Parameters
    ----------
    node
        The starting node

    address
        Address of a nested node. (e.g. ['three', 'nested', 'levels']).

    parents
        When invoked recursively, a list of the parents of `node`.
    """
    current_namespace = ".".join(_nodes_referenced_so_far(parent) + [node_name])

    if len(address) == 0:
        return NodeWithParent(
            node=node,
            node_name=node_name,
            parent=parent,
        )

    if isinstance(node, Task):
        raise ValueError(
            f"Node '{current_namespace}' does not contain any other nodes. However, you are trying to access a subnode '{'.'.join(address)}'."
        )

    dag = node
    next_node = address[0]
    if next_node not in dag.nodes:
        human_readable_dag_address = (
            f"DAG '{current_namespace}'" if parent else "this DAG"
        )
        human_readable_node_address = (
            f"{current_namespace}.{next_node}" if parent else next_node
        )
        raise ValueError(
            f"You selected node '{human_readable_node_address}'. However, {human_readable_dag_address} does not contain any node with such a name. These are the names the DAG contains: {sorted(list(dag.nodes))}"
        )

    return find_nested_node(
        dag.nodes[next_node],
        address=address[1:],
        node_name=next_node,
        parent=NodeWithParent(
            node=dag,
            node_name=node_name,
            parent=parent,
        ),
    )
