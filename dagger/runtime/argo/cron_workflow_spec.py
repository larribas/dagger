"""Generate CronWorkflow specifications."""

from typing import Any, Mapping

from dagger.dag import DAG
from dagger.runtime.argo.cron import Cron
from dagger.runtime.argo.extra_spec_options import with_extra_spec_options
from dagger.runtime.argo.workflow import Workflow
from dagger.runtime.argo.workflow_spec import workflow_spec


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
