from dagger import dsl
from dagger.runtime.argo import Metadata, Workflow, workflow_manifest


@dsl.task()
def say_hello():
    print("Hello!")


@dsl.DAG()
def dag():
    say_hello()


manifest = workflow_manifest(
    dsl.build(dag),
    metadata=Metadata(name="my-pipeline"),
    workflow=Workflow(
        container_image="my-docker-registry/my-image:my-tag",
        extra_spec_options={
            # Available options to override: https://argoproj.github.io/argo-workflows/fields/#workflowspec
            "priority": 100,
            "nodeSelector": {
                "my-label": "my-value",
            },
        },
    ),
)
