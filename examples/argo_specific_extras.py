"""
# Argo-specific Extras.

This example shows how we can provide extra options to the Argo runtime.

This example may only show a subset of all the available options. Check the Argo [template specification](https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template) and the different data structures provided by `dagger.runtime.argo.options` to understand what's possible.
"""
from dagger import DAG, Task


def long_task():  # noqa
    print("Long task that benefits from a timeout and a retry strategy")


dag = DAG(
    {
        "long-task": Task(
            long_task,
            runtime_options={
                "argo_container_overrides": {
                    "resources": {"requests": {"cpu": "100m", "memory": "60Mi"}}
                },
                "argo_template_overrides": {
                    "retryStrategy": {"limit": 5},
                    "activeDeadlineSeconds": 30,
                },
            },
        ),
    },
    runtime_options={
        "argo_dag_template_overrides": {
            "failFast": False,
        },
    },
)


if __name__ == "__main__":
    """Define a command-line interface for this DAG, using the CLI runtime. Check the documentation to understand why this is relevant or necessary."""
    from dagger.runtime.cli import invoke

    invoke(dag)
