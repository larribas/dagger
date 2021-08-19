from dagger.dag import DAG
from dagger.runtime.argo import (
    Cron,
    CronConcurrencyPolicy,
    Metadata,
    cluster_workflow_template_manifest,
    cron_workflow_manifest,
    workflow_manifest,
    workflow_template_manifest,
)
from dagger.runtime.argo.cron_workflow_spec import cron_workflow_spec
from dagger.runtime.argo.workflow_spec import Workflow, workflow_spec
from dagger.task import Task


def test__workflow_manifest():
    dag = DAG(nodes=dict(single=Task(lambda: 1)))
    metadata = Metadata(name="my-workflow")
    workflow = Workflow(container_image="my-image")

    manifest = workflow_manifest(
        dag,
        metadata=metadata,
        workflow=workflow,
    )

    assert "apiVersion" in manifest
    assert manifest["kind"] == "Workflow"
    assert manifest["metadata"]["name"] == "my-workflow"
    assert manifest["spec"] == workflow_spec(dag, workflow)


def test__workflow_template_manifest():
    dag = DAG(nodes=dict(single=Task(lambda: 1)))
    metadata = Metadata(name="my-workflow-template")
    workflow = Workflow(container_image="my-image")

    manifest = workflow_template_manifest(
        dag,
        metadata=metadata,
        workflow=workflow,
    )

    assert "apiVersion" in manifest
    assert manifest["kind"] == "WorkflowTemplate"
    assert manifest["metadata"]["name"] == "my-workflow-template"
    assert manifest["spec"] == workflow_spec(dag, workflow)


def test__cluster_workflow_template_manifest():
    dag = DAG(nodes=dict(single=Task(lambda: 1)))
    metadata = Metadata(name="my-cluster-workflow-template")
    workflow = Workflow(container_image="my-image")

    manifest = cluster_workflow_template_manifest(
        dag,
        metadata=metadata,
        workflow=workflow,
    )

    assert "apiVersion" in manifest
    assert manifest["kind"] == "ClusterWorkflowTemplate"
    assert manifest["metadata"]["name"] == "my-cluster-workflow-template"
    assert manifest["spec"] == workflow_spec(dag, workflow)


def test__cron_workflow_manifest():
    dag = DAG(nodes=dict(single=Task(lambda: 1)))
    metadata = Metadata(name="my-cron-workflow")
    workflow = Workflow(container_image="my-image")
    cron = Cron(schedule="* * * * *")
    manifest = cron_workflow_manifest(
        dag,
        metadata=metadata,
        cron=cron,
        workflow=workflow,
    )

    assert "apiVersion" in manifest
    assert manifest["kind"] == "CronWorkflow"
    assert manifest["metadata"]["name"] == "my-cron-workflow"
    assert manifest["spec"] == cron_workflow_spec(dag, cron=cron, workflow=workflow)
