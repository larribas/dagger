from dagger.dag import DAG
from dagger.runtime.argo.cron_workflow_spec import (
    Cron,
    CronConcurrencyPolicy,
    cron_workflow_spec,
)
from dagger.runtime.argo.workflow_spec import Workflow, workflow_spec
from dagger.task import Task


def test__cron_workflow_spec__simplest_form():
    dag = DAG({"single-node": Task(lambda: 1)})
    cron = Cron(schedule="1 2 3 * *")
    workflow = Workflow(container_image="my-image")

    assert cron_workflow_spec(dag=dag, cron=cron, workflow=workflow) == {
        "schedule": "1 2 3 * *",
        "startingDeadlineSeconds": 0,
        "concurrencyPolicy": "Allow",
        "workflowSpec": workflow_spec(dag, workflow),
    }


def test__cron_workflow_spec__overriding_defaults():
    dag = DAG({"single-node": Task(lambda: 1)})
    cron = Cron(
        schedule="1 2 3 4 5",
        starting_deadline_seconds=3,
        concurrency_policy=CronConcurrencyPolicy.FORBID,
        timezone="Europe/Madrid",
        successful_jobs_history_limit=11,
        failed_jobs_history_limit=22,
        extra_spec_options={"suspend": True, "extraArg": "value"},
    )
    workflow = Workflow(container_image="my-image")
    assert cron_workflow_spec(dag=dag, cron=cron, workflow=workflow) == {
        "schedule": "1 2 3 4 5",
        "startingDeadlineSeconds": 3,
        "concurrencyPolicy": "Forbid",
        "timezone": "Europe/Madrid",
        "successfulJobsHistoryLimit": 11,
        "failedJobsHistoryLimit": 22,
        "suspend": True,
        "extraArg": "value",
        "workflowSpec": workflow_spec(dag, workflow),
    }
