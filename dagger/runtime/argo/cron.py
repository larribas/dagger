"""Cofiguration for a Cron Workflow."""
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


class Cron:
    """
    Scheduling options for the cron job.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#cronworkflowspec
    """

    def __init__(
        self,
        schedule: str,
        starting_deadline_seconds: int = 0,
        concurrency_policy: CronConcurrencyPolicy = CronConcurrencyPolicy.ALLOW,
        timezone: Optional[str] = None,
        successful_jobs_history_limit: Optional[int] = None,
        failed_jobs_history_limit: Optional[int] = None,
        extra_spec_options: Mapping[str, Any] = None,
    ):
        """
        Initialize the configuration for a cron workflow.

        Parameters
        ----------
        schedule: str
            The schedule the workflow should run according to.

        starting_deadline_seconds: int, default=0
            The time to wait for a scheduled workflow to start. If the original schedule + deadline are missed, the workflow will be cancelled.

        concurrency_policy: CronConcurrencyPolicy, default=REPLACE
            The kubernetes concurrency policy to use. Check https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/cron-job-v1/

        timezone: str, optional
            The timezone to use when scheduling the workflow.

        successful_jobs_history_limit: int, optional
            The limit of jobs (which have ended successfully) to keep in the history.

        failed_jobs_history_limit: int, optional
            The limit of jobs (which have ended with a failure) to keep in the history.

        extra_spec_options: Mapping[str, Any], default={}
            CronWorkflowSpec properties to set (if they are not used by the runtime).

        """
        self._schedule = schedule
        self._starting_deadline_seconds = starting_deadline_seconds
        self._concurrency_policy = concurrency_policy
        self._timezone = timezone
        self._successful_jobs_history_limit = successful_jobs_history_limit
        self._failed_jobs_history_limit = failed_jobs_history_limit
        self._extra_spec_options = extra_spec_options or {}

    @property
    def schedule(self) -> str:
        """Return the schedule for this cron workflow."""
        return self._schedule

    @property
    def starting_deadline_seconds(self) -> int:
        """Return the starting deadline for this cron workflow, in seconds."""
        return self._starting_deadline_seconds

    @property
    def concurrency_policy(self) -> CronConcurrencyPolicy:
        """Return the concurrency policy for this cron workflow."""
        return self._concurrency_policy

    @property
    def timezone(self) -> Optional[str]:
        """Return the timezone this cron workflow should be scheduled according to, if any."""
        return self._timezone

    @property
    def successful_jobs_history_limit(self) -> Optional[int]:
        """Return the limit of successful jobs to keep a historical record for, if defined."""
        return self._successful_jobs_history_limit

    @property
    def failed_jobs_history_limit(self) -> Optional[int]:
        """Return the limit of failed jobs to keep a historical record for, if defined."""
        return self._failed_jobs_history_limit

    @property
    def extra_spec_options(self) -> Mapping[str, Any]:
        """Return any extra options that should be passed to the Cron spec."""
        return self._extra_spec_options

    def __repr__(self) -> str:
        """Return a human-readable representation of this instance."""
        return f"Cron(schedule={self._schedule}, starting_deadline_seconds={self._starting_deadline_seconds}, concurrency_policy={self._concurrency_policy.value}, timezone={self._timezone}, successful_jobs_history_limit={self._successful_jobs_history_limit}, failed_jobs_history_limit={self._failed_jobs_history_limit}, extra_spec_options={self._extra_spec_options})"

    def __eq__(self, obj) -> bool:
        """Return true if the object is equivalent to the current instance."""
        return (
            isinstance(obj, Cron)
            and self._schedule == obj._schedule
            and self._starting_deadline_seconds == obj._starting_deadline_seconds
            and self._concurrency_policy == obj._concurrency_policy
            and self._timezone == obj._timezone
            and self._successful_jobs_history_limit
            == obj._successful_jobs_history_limit
            and self._failed_jobs_history_limit == obj._failed_jobs_history_limit
            and self._extra_spec_options == obj._extra_spec_options
        )
