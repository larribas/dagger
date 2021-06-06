from dagger.runtime.argo.compiler.cron_workflow_spec import (
    Cron,
    CronConcurrencyPolicy,
    cron_workflow_spec,
)


def test__cron_workflow_spec__simplest_form():
    cron = Cron(schedule="* * * * *")
    assert cron_workflow_spec(cron, {"valid": "spec"}) == {
        "schedule": "* * * * *",
        "suspend": False,
        "startingDeadlineSeconds": 0,
        "concurrencyPolicy": "Allow",
        "workflowSpec": {"valid": "spec"},
    }


def test__cron_workflow_spec__overriding_defaults():
    cron = Cron(
        schedule="1 2 3 4 5",
        suspend=True,
        starting_deadline_seconds=3,
        concurrency_policy=CronConcurrencyPolicy.FORBID,
        timezone="Europe/Madrid",
        successful_jobs_history_limit=11,
        failed_jobs_history_limit=22,
    )
    assert cron_workflow_spec(cron, {"valid": "spec"}) == {
        "schedule": "1 2 3 4 5",
        "suspend": True,
        "startingDeadlineSeconds": 3,
        "concurrencyPolicy": "Forbid",
        "timezone": "Europe/Madrid",
        "successfulJobsHistoryLimit": 11,
        "failedJobsHistoryLimit": 22,
        "workflowSpec": {"valid": "spec"},
    }
