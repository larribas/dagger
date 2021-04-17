from typing import Any, Dict, List, Set


class CyclicDependencyError(Exception):
    pass


def topological_sort(node_dependencies: Dict[Any, Set[Any]]) -> List[Set[Any]]:
    sorted_sets = []

    remaining_node_dependencies = {
        node: dependencies
        for node, dependencies in node_dependencies.items()
        if len(dependencies) != 0
    }

    while len(remaining_node_dependencies) != 0:
        remaining_nodes = all_nodes(remaining_node_dependencies)
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

    last_set = all_nodes(node_dependencies) - set().union(*sorted_sets)
    if len(last_set) != 0:
        sorted_sets.append(last_set)

    return sorted_sets


def all_nodes(node_dependencies: Dict[Any, Set[Any]]):
    return set(node_dependencies.keys()).union(*node_dependencies.values())
