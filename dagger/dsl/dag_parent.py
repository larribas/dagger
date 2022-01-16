"""Relevant information about the parent of a DAG."""

from typing import Mapping

from dagger.dsl.node_invocations import NodeInputReference


class DAGParent:
    """Contains information about a DAG's parent to allow us to build the child DAG."""

    def __init__(
        self,
        inputs: Mapping[str, NodeInputReference],
        node_names_by_id: Mapping[str, str],
    ):
        self._inputs = inputs
        self._node_names_by_id = node_names_by_id

    @property
    def inputs(self) -> Mapping[str, NodeInputReference]:
        """Return the name of the key this reference points to."""
        return self._inputs

    @property
    def node_names_by_id(self) -> Mapping[str, str]:
        """Return a mapping of node IDs to unique node names."""
        return self._node_names_by_id

    def __repr__(self) -> str:
        """Get a human-readable string representation of this object."""
        return f"DAGParent(inputs={self._inputs}, node_names_by_id={self._node_names_by_id})"

    def __eq__(self, obj) -> bool:
        """Return true if both objects are equivalent."""
        return (
            isinstance(obj, DAGParent)
            and self._inputs == obj._inputs
            and self._node_names_by_id == obj._node_names_by_id
        )
