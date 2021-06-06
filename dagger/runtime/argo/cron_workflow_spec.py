"""Generate CronWorkflow specifications."""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional


class CronConcurrencyPolicy(Enum):
    """
    Concurrency policies allowed by Argo/Kubernetes.

    Docs: https://v1-20.docs.kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/#concurrency-policy
    """

    ALLOW = "Allow"
    FORBID = "Forbid"
    REPLACE = "Replace"


# TODO: Change to a NamedTuple
@dataclass
class Cron:
    """
    Scheduling options for the cron job.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#cronworkflowspec
    """

    schedule: str
    suspend: bool = False
    starting_deadline_seconds: int = 0
    concurrency_policy: CronConcurrencyPolicy = CronConcurrencyPolicy.ALLOW
    timezone: Optional[str] = None
    successful_jobs_history_limit: Optional[int] = None
    failed_jobs_history_limit: Optional[int] = None


def cron_workflow_spec(
    cron: Cron,
    workflow_spec: Mapping[str, Any],
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a CronWorkflowSpec with the supplied parameters.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#cronworkflowspec

    Parameters
    ----------
    workflow_spec
        A well-formed WorkflowSpec.
        Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflowspec

    Returns
    -------
    A CronWorkflowSpec represented as a Python mapping
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
