"""
Extra options you may provide specify in your DAGs/Tasks.

These options allow you to manipulate specific features/properties of Argo (such as timeouts or retry strategies) and Kubernetes (such as resource requests/limits, and node affinities and tolerations).

You may supply these options like this:

Task(..., runtime_options=[ArgoTaskOptions(...)])
DAG(..., runtime_options=[ArgoDAGOptions(...)])
"""

from dataclasses import dataclass
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class ArgoTaskOptions:
    """
    Extra options for a Task.

    We don't want dagger to fall behind in terms of compatibility with the latest features of Argo.

    This class allows you to specify custom overrides for these specs:
    - Template: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    - Container: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#container

    And declare these overrides together with the Task you are defining, so that there's a single source of truth for the behavior of a Task. Or put another way:
    > Gather together those things that change for the same reason
    """

    template_overrides: Optional[Mapping[str, Any]] = None
    container_overrides: Optional[Mapping[str, Any]] = None


@dataclass(frozen=True)
class ArgoDAGOptions:
    """
    Extra options for a DAG.

    We don't want dagger to fall behind in terms of compatibility with the latest features of Argo.

    This class allows you to specify custom overrides for these specs:
    - DAGTemplate: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#dagtemplate

    And declare these overrides together with the DAG you are defining, so that there's a single source of truth for the behavior of a DAG. Or put another way:
    > Gather together those things that change for the same reason
    """

    dag_template_overrides: Optional[Mapping[str, Any]] = None
