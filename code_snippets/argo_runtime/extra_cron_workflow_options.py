from dagger import dsl
from dagger.runtime.argo import (
    Cron,
    CronConcurrencyPolicy,
    Metadata,
    Workflow,
    cron_workflow_manifest,
)


@dsl.task()
def say_hello():
    print("Hello!")


@dsl.DAG()
def dag():
    say_hello()


manifest = cron_workflow_manifest(
    dsl.build(dag),
    metadata=Metadata(name="my-pipeline"),
    workflow=Workflow(container_image="my-docker-registry/my-image:my-tag"),
    cron=Cron(
        schedule="0 0 * * *",
        starting_deadline_seconds=60,
        concurrency_policy=CronConcurrencyPolicy.FORBID,
        timezone="Europe/Barcelona",
        successful_jobs_history_limit=10,
        failed_jobs_history_limit=50,
        extra_spec_options={
            # Available options to override: https://argoproj.github.io/argo-workflows/fields/#cronworkflowspec
            "workflowMetadata": {
                "annotations": {
                    "my-annotation": "my-value",
                },
            },
        },
    ),
)
