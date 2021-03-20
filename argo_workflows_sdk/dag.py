import logging
import re
from typing import List, Optional

from argo_workflows_sdk.node import Node

VALID_NAME = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,128}$")


class DAG:
    def __init__(
        self,
        name: str,
        nodes: List[Node],
    ):
        # TODO: Guarantee this is a valid name
        self.name = name
        # TODO: When we allow specifying inputs/outputs, perform a topological sort
        # TODO: When we allow composing DAGs, flatten the nodes and arrange for all the names to be unique
        self.nodes = nodes

    # TODO: Allow calling this from Python code without attempting to parse any arguments
    def __call__(self):
        import argparse

        parser = argparse.ArgumentParser(
            description="Run a DAG, either completely, or partially using the filters specified in the arguments"
        )
        parser.add_argument(
            "--node-name",
            type=str,
            help="Select a specific node to run. It must be properly namespaced with the name of all the parent DAGs.",
        )
        args = parser.parse_args()
        logging.debug(f"Arguments passed to DAG are {args}")

        nodes_to_run = self._filter_nodes(args.node_name)

        for node in nodes_to_run:
            node()

    def _filter_nodes(self, node_name: Optional[str] = None) -> List[Node]:
        nodes = self.nodes

        if node_name:
            nodes = [n for n in self.nodes if n.name == node_name]
            if nodes == []:
                logging.debug(
                    f"DAG {self.name} has no nodes named {node_name}.\nThe available nodes are {self._node_names()}"
                )

        return nodes

    def _node_names(self) -> List[str]:
        return ", ".join([n.name for n in self.nodes])
