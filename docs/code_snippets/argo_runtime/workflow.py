from dagger import dsl


@dsl.task()
def echo(message):
    print(message)


@dsl.DAG()
def my_pipeline(message1, message2):
    echo(message1)
    echo(message2)


# When using the DSL, remember to ALWAYS call dsl.build()
dag = dsl.build(my_pipeline)


from dagger.runtime.argo import Metadata, Workflow, workflow_manifest

manifest = workflow_manifest(
    dag,
    metadata=Metadata(
        name="my-pipeline",
    ),
    workflow=Workflow(
        container_image="my-docker-registry/my-image:my-tag",
        container_entrypoint_to_dag_cli=["python", "dag_exposed_via_cli_runtime.py"],
        params={
            "message1": "Hello",
            "message2": "World",
        },
    ),
)
