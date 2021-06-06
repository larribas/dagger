"""
Extra options you may provide specify in your DAGs/Tasks.

These options allow you to manipulate specific features/properties of Argo (such as timeouts or retry strategies) and Kubernetes (such as resource requests/limits, and node affinities and tolerations).

You may supply these options like this:

Task(..., runtime_options=[ArgoTaskOptions(...)])
DAG(..., runtime_options=[ArgoDAGOptions(...)])
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ArgoTaskOptions:
    """
    Extra options for a Task template.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """

    timeout_seconds: Optional[int] = None
    active_deadline_seconds: Optional[int] = None


@dataclass(frozen=True)
class ArgoDAGOptions:
    """
    Extra options for a DAG template.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#dagtemplate
    """

    fail_fast: bool = True
