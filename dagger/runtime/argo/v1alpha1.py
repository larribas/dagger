"""Functions that compile a DAG into Argo CRDs (e.g. Workflow and CronWorkflow) using the v1alpha1 API version."""

from typing import Any, Mapping

from dagger.dag import DAG
from dagger.runtime.argo.cron_workflow_spec import Cron, cron_workflow_spec
from dagger.runtime.argo.metadata import Metadata, object_meta
from dagger.runtime.argo.workflow_spec import Workflow, workflow_spec

API_VERSION = "argoproj.io/v1alpha1"


def workflow_manifest(
    dag: DAG,
    metadata: Metadata,
    workflow: Workflow,
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a Workflow to execute the supplied DAG with the specified metadata.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflow


    Parameters
    ----------
    dag
        The DAG to convert into an Argo Workflow

    metadata
        Kubernetes metadata (name, namespace, labels, ...) to inject to the workflow

    workflow
        Workflow configuration (parameters, container image and entrypoint, ...)


    Raises
    ------
    ValueError
        If any of the extra_spec_options collides with a property used by the runtime.
    """
    return {
        "apiVersion": API_VERSION,
        "kind": "Workflow",
        "metadata": object_meta(metadata),
        "spec": workflow_spec(dag, workflow),
    }


def workflow_template_manifest(
    dag: DAG,
    metadata: Metadata,
    workflow: Workflow,
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a WorkflowTemplate to execute the supplied DAG with the specified metadata.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflowtemplate

    To see the parameters required and exceptions raised by this function, please refer to the `workflow_manifest` function.
    """
    return {
        "apiVersion": API_VERSION,
        "kind": "WorkflowTemplate",
        "metadata": object_meta(metadata),
        "spec": workflow_spec(dag, workflow),
    }


def cluster_workflow_template_manifest(
    dag: DAG,
    metadata: Metadata,
    workflow: Workflow,
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a ClusterWorkflowTemplate to execute the supplied DAG with the specified metadata.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflowtemplate

    To see the parameters required and exceptions raised by this function, please refer to the `workflow_manifest` function.
    """
    return {
        "apiVersion": API_VERSION,
        "kind": "ClusterWorkflowTemplate",
        "metadata": object_meta(metadata),
        "spec": workflow_spec(dag, workflow),
    }


def cron_workflow_manifest(
    dag: DAG,
    metadata: Metadata,
    workflow: Workflow,
    cron: Cron,
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a CronWorkflow to execute the supplied DAG with the specified metadata and scheduling parameters.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#cronworkflow

    Parameters
    ----------
    dag
        The DAG to convert into an Argo Workflow

    metadata
        Kubernetes metadata (name, namespace, labels, ...) to inject to the workflow

    workflow
        Workflow configuration (parameters, container image and entrypoint, ...)

    cron
        Cron configuration (schedule, concurrency, ...)


    Raises
    ------
    ValueError
        If any of the extra_spec_options collides with a property used by the runtime.
    """
    return {
        "apiVersion": API_VERSION,
        "kind": "CronWorkflow",
        "metadata": object_meta(metadata),
        "spec": cron_workflow_spec(
            dag=dag,
            cron=cron,
            workflow=workflow,
        ),
    }
