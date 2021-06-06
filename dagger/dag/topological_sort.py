"""Sort nodes topologically by their dependencies and detect possible cyclic dependencies."""
from typing import Any, List, Mapping, Set, TypeVar

T = TypeVar("T")


class CyclicDependencyError(Exception):
    """Error raised when a DAG contains a cyclic dependency, and thus cannot be executed."""

    pass


def topological_sort(node_dependencies: Mapping[T, Set[T]]) -> List[Set[T]]:
    """
    Perform a topological sort of the provided set of dependencies.

    Parameters
    ----------
    node_dependencies : A mapping from T to Set[T], where T must be hashable
        Each key in the mapping represents a node.
        Each value is a set of nodes that should be executed before the current one.
        The dictionary doesn't have to be exhaustive. That is, if a node is mentioned as a dependency of another, but is not present as a key in the dictionary, an empty set of dependencies is assumed from it.

    Returns
    -------
    List of Sets of T
        Each set contains nodes that can be executed concurrently.
        The list determines the right order of execution.
    """
    sorted_sets = []

    remaining_node_dependencies = {
        node: dependencies
        for node, dependencies in node_dependencies.items()
        if len(dependencies) != 0
    }

    while len(remaining_node_dependencies) != 0:
        remaining_nodes = _all_nodes(remaining_node_dependencies)
        nodes_with_no_incoming_dependencies = remaining_nodes - set(
            remaining_node_dependencies.keys()
        )

        if len(nodes_with_no_incoming_dependencies) == 0:
            raise CyclicDependencyError(
                f"There is a cyclic dependency between the following nodes: {remaining_nodes}"
            )

        sorted_sets.append(nodes_with_no_incoming_dependencies)

        remaining_node_dependencies = {
            node: dependencies - nodes_with_no_incoming_dependencies
            for node, dependencies in remaining_node_dependencies.items()
            if len(dependencies - nodes_with_no_incoming_dependencies) != 0
        }

    last_set = _all_nodes(node_dependencies) - set().union(*sorted_sets)
    if len(last_set) != 0:
        sorted_sets.append(last_set)

    return sorted_sets


def _all_nodes(node_dependencies: Mapping[Any, Set[Any]]):
    return set(node_dependencies.keys()).union(*node_dependencies.values())
