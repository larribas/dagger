"""Generate CronWorkflow specifications."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional

from dagger.dag import DAG
from dagger.runtime.argo.extra_spec_options import with_extra_spec_options
from dagger.runtime.argo.workflow_spec import Workflow, workflow_spec


class CronConcurrencyPolicy(Enum):
    """
    Concurrency policies allowed by Argo/Kubernetes.

    Docs: https://v1-20.docs.kubernetes.io/docs/tasks/job/automated-tasks-with-cron-jobs/#concurrency-policy
    """

    ALLOW = "Allow"
    FORBID = "Forbid"
    REPLACE = "Replace"


@dataclass(frozen=True)
class Cron:
    """
    Scheduling options for the cron job.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#cronworkflowspec
    """

    schedule: str
    starting_deadline_seconds: int = 0
    concurrency_policy: CronConcurrencyPolicy = CronConcurrencyPolicy.ALLOW
    timezone: Optional[str] = None
    successful_jobs_history_limit: Optional[int] = None
    failed_jobs_history_limit: Optional[int] = None
    extra_spec_options: Mapping[str, Any] = field(default_factory=dict)


def cron_workflow_spec(
    dag: DAG,
    workflow: Workflow,
    cron: Cron,
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a CronWorkflowSpec with the supplied parameters.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#cronworkflowspec

    Parameters
    ----------
    dag
        The DAG to generate the spec for

    workflow
        The configuration for this workflow

    cron
        The configuration for the cron workflow


    Raises
    ------
    ValueError
        If any of the cron.extra_spec_options collides with a property used by the runtime.
    """
    spec = {
        "schedule": cron.schedule,
        "startingDeadlineSeconds": cron.starting_deadline_seconds,
        "concurrencyPolicy": cron.concurrency_policy.value,
        "workflowSpec": workflow_spec(dag, workflow),
    }

    if cron.timezone:
        spec["timezone"] = cron.timezone

    if cron.successful_jobs_history_limit:
        spec["successfulJobsHistoryLimit"] = cron.successful_jobs_history_limit

    if cron.failed_jobs_history_limit:
        spec["failedJobsHistoryLimit"] = cron.failed_jobs_history_limit

    spec = with_extra_spec_options(
        original=spec,
        extra_options=cron.extra_spec_options,
        context="the CronWorkflow spec",
    )

    return spec
