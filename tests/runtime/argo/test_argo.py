from dagger.dag import DAG
from dagger.runtime.argo import (
    Cron,
    CronConcurrencyPolicy,
    Metadata,
    cron_workflow_manifest,
    workflow_manifest,
)
from dagger.task import Task


def test__workflow_manifest():
    dag = DAG(nodes=dict(single=Task(lambda: 1)))
    manifest = workflow_manifest(
        dag,
        metadata=Metadata(name="my-workflow"),
        container_image="my-image",
    )

    assert "apiVersion" in manifest
    assert manifest["kind"] == "Workflow"
    assert "metadata" in manifest
    assert manifest["metadata"]["name"] == "my-workflow"
    assert "spec" in manifest
    assert manifest["spec"] != {}


def test__cron_workflow_manifest():
    dag = DAG(nodes=dict(single=Task(lambda: 1)))
    manifest = cron_workflow_manifest(
        dag,
        metadata=Metadata(
            name="my-workflow",
            generate_name_from_prefix=True,
        ),
        cron=Cron(
            schedule="@yearly",
            concurrency_policy=CronConcurrencyPolicy.REPLACE,
        ),
        container_image="my-image",
    )

    assert "apiVersion" in manifest
    assert manifest["kind"] == "CronWorkflow"
    assert "metadata" in manifest
    assert manifest["metadata"]["generateName"] == "my-workflow"
    assert manifest["spec"]["schedule"] == "@yearly"
    assert manifest["spec"]["concurrencyPolicy"] == "Replace"
    assert manifest["spec"]["workflowSpec"] != {}
