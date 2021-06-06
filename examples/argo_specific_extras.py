"""
# Argo-specific Extras.

This example shows how we can provide extra options to the Argo runtime.

This example may only show a subset of all the available options. Check the Argo [template specification](https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template) and the different data structures provided by `dagger.runtime.argo.options` to understand what's possible.
"""
import time

from dagger import DAG, Task
from dagger.runtime.argo.options import ArgoTaskOptions

dag = DAG(
    {
        "long-task-with-timeout": Task(
            lambda: time.sleep(40),
            runtime_options=[
                ArgoTaskOptions(
                    timeout_seconds=30,
                    active_deadline_seconds=20,
                ),
            ],
        ),
    },
)


def run_from_cli():
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dag)
