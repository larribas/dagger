"""
Extra options you may provide specify in your DAGs/Tasks.

These options allow you to manipulate specific features/properties of Argo (such as timeouts or retry strategies) and Kubernetes (such as resource requests/limits, and node affinities and tolerations).

You may supply these options like this:

Task(..., runtime_options=[ArgoTaskOptions(...)])
DAG(..., runtime_options=[ArgoDAGOptions(...)])
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class RetryPolicy(Enum):
    """
    Retry policy for a task.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#retrystrategy
    """

    ALWAYS = "Always"
    ON_FAILURE = "OnFailure"
    ON_ERROR = "OnError"
    ON_TRANSIENT_ERROR = "OnTransientError"


@dataclass(frozen=True)
class RetryBackoff:
    """
    Retry strategy backoff policy.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#backoff
    """

    duration_seconds: int
    max_duration_seconds: int
    factor: int


@dataclass(frozen=True)
class RetryStrategy:
    """
    Retry strategy.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#retrystrategy
    """

    limit: Optional[int] = None
    policy: RetryPolicy = RetryPolicy.ON_FAILURE
    node_anti_affinity: bool = False
    backoff: Optional[RetryBackoff] = None


@dataclass(frozen=True)
class ArgoTaskOptions:
    """
    Extra options for a Task template.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """

    timeout_seconds: Optional[int] = None
    active_deadline_seconds: Optional[int] = None
    retry_strategy: Optional[RetryStrategy] = None
    service_account: Optional[str] = None
    parallelism: Optional[int] = None
    priority: Optional[int] = None


@dataclass(frozen=True)
class ArgoDAGOptions:
    """
    Extra options for a DAG template.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#dagtemplate
    """

    fail_fast: bool = True
