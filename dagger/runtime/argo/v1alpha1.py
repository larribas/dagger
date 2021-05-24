from typing import Dict, List, Optional

from dagger.dag import DAG
from dagger.runtime.argo.cron_workflow_spec import Cron, cron_workflow_spec
from dagger.runtime.argo.metadata import Metadata, object_meta
from dagger.runtime.argo.workflow_spec import workflow_spec

API_VERSION = "argoproj.io/v1alpha1"


def workflow_manifest(
    dag: DAG,
    metadata: Metadata,
    container_image: str,
    params: Optional[Dict[str, bytes]] = None,
    container_entrypoint_to_dag_cli: Optional[List[str]] = None,
    service_account: Optional[str] = None,
) -> dict:
    """
    Returns a minimal representation of a Workflow to execute the supplied DAG
    with the specified metadata
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflow
    """
    return {
        "apiVersion": API_VERSION,
        "kind": "Workflow",
        "metadata": object_meta(metadata),
        "spec": workflow_spec(
            dag=dag,
            params=params,
            container_image=container_image,
            container_entrypoint_to_dag_cli=container_entrypoint_to_dag_cli,
            service_account=service_account,
        ),
    }


def cron_workflow_manifest(
    dag: DAG,
    metadata: Metadata,
    cron: Cron,
    container_image: str,
    params: Optional[Dict[str, bytes]] = None,
    container_entrypoint_to_dag_cli: Optional[List[str]] = None,
    service_account: Optional[str] = None,
) -> dict:
    """
    Returns a minimal representation of a CronWorkflow to execute the supplied DAG
    with the specified metadata and scheduling parameters.
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#cronworkflow
    """
    return {
        "apiVersion": API_VERSION,
        "kind": "CronWorkflow",
        "metadata": object_meta(metadata),
        "spec": cron_workflow_spec(
            cron=cron,
            workflow_spec=workflow_spec(
                dag=dag,
                params=params,
                container_image=container_image,
                container_entrypoint_to_dag_cli=container_entrypoint_to_dag_cli,
                service_account=service_account,
            ),
        ),
    }
