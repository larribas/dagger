from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CronConcurrencyPolicy(Enum):
    ALLOW = "Allow"
    FORBID = "Forbid"
    REPLACE = "Replace"


@dataclass
class Cron:
    schedule: str
    suspend: bool = False
    starting_deadline_seconds: int = 0
    concurrency_policy: CronConcurrencyPolicy = CronConcurrencyPolicy.ALLOW
    timezone: Optional[str] = None
    successful_jobs_history_limit: Optional[int] = None
    failed_jobs_history_limit: Optional[int] = None


def cron_workflow_spec(cron: Cron, workflow_spec: dict):
    """
    Returns a minimal representation of a CronWorkflowSpec with the supplied parameters.
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#cronworkflowspec

    It assumes workflow_spec is a well-formed WorkflowSpec.
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflowspec
    """
    spec = {
        "schedule": cron.schedule,
        "suspend": cron.suspend,
        "startingDeadlineSeconds": cron.starting_deadline_seconds,
        "concurrencyPolicy": cron.concurrency_policy.value,
        "workflowSpec": workflow_spec,
    }

    if cron.timezone:
        spec["timezone"] = cron.timezone

    if cron.successful_jobs_history_limit:
        spec["successfulJobsHistoryLimit"] = cron.successful_jobs_history_limit

    if cron.failed_jobs_history_limit:
        spec["failedJobsHistoryLimit"] = cron.failed_jobs_history_limit

    return spec
