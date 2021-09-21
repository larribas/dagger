"""Configuration for a Workflow."""

from typing import Any, List, Mapping


class Workflow:
    """
    Configuration for a Workflow.

    This class will be supplied to the runtime to help it create a workflow that will work on your environment.
    """

    def __init__(
        self,
        container_image: str,
        container_entrypoint_to_dag_cli: List[str] = None,
        params: Mapping[str, Any] = None,
        extra_spec_options: Mapping[str, Any] = None,
    ):
        """
        Create a workflow configuration.

        Parameters
        ----------
        container_image: str
            The URI to the container image Argo will use for every node in the DAG.

        container_entrypoint_to_dag_cli: List[str], default=[]
            The container entrypoint Argo will use for every node in the DAG.
            The entrypoint must expose the CLI runtime for the same DAG that was used to generate the Argo manifests.
            Check the section about the Argo runtime to understand better how to containerize your projects and expose your DAGs.

        params: Mapping[str, Any], default={}
            Parameters to inject to the DAG.
            They must match the inputs the DAG expects.

        extra_spec_options, Mapping[str, Any], default={}
            WorkflowSpec properties to set (if they are not used by the runtime).
        """
        self._container_image = container_image
        self._container_entrypoint_to_dag_cli = container_entrypoint_to_dag_cli or []
        self._params = params or {}
        self._extra_spec_options = extra_spec_options or {}

    @property
    def container_image(self) -> str:
        """Return the container image that will be used to run every step in this workflow."""
        return self._container_image

    @property
    def container_entrypoint_to_dag_cli(self) -> List[str]:
        """
        Return the container entrypoint that will be used in order to run every step in this workflow.

        The entrypoint must expose the CLI runtime for the same DAG that was used to generate the Argo manifests.
        """
        return self._container_entrypoint_to_dag_cli

    @property
    def params(self) -> Mapping[str, Any]:
        """Return the parameters to supply to the workflow or workflow template."""
        return self._params

    @property
    def extra_spec_options(self) -> Mapping[str, Any]:
        """Return any extra options that should be passed to the WorkflowSpec."""
        return self._extra_spec_options

    def __repr__(self) -> str:
        """Return a human-readable representation of this instance."""
        return f"Workflow(container_image={self._container_image}, container_entrypoint_to_dag_cli={self._container_entrypoint_to_dag_cli}, params={self._params}, extra_spec_options={self._extra_spec_options})"

    def __eq__(self, obj) -> bool:
        """Return true if the object is equivalent to the current instance."""
        return (
            isinstance(obj, Workflow)
            and self._container_image == obj._container_image
            and self._container_entrypoint_to_dag_cli
            == obj._container_entrypoint_to_dag_cli
            and self._params == obj._params
            and self._extra_spec_options == obj._extra_spec_options
        )
