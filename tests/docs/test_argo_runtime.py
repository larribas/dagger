def test_workflow():
    from docs.code_snippets.argo_runtime.workflow import manifest

    assert manifest["kind"] == "Workflow"
    assert len(manifest["spec"]["templates"]) == 3


def test_workflow_template():
    from docs.code_snippets.argo_runtime.workflow_template import manifest

    assert manifest["kind"] == "WorkflowTemplate"
    assert len(manifest["spec"]["templates"]) == 3


def test_cluster_workflow_template():
    from docs.code_snippets.argo_runtime.cluster_workflow_template import manifest

    assert manifest["kind"] == "ClusterWorkflowTemplate"
    assert len(manifest["spec"]["templates"]) == 3


def test_cron_workflow():
    from docs.code_snippets.argo_runtime.cron_workflow import manifest

    assert manifest["kind"] == "CronWorkflow"
    assert len(manifest["spec"]["workflowSpec"]["templates"]) == 3


def test_extra_container_options():
    from docs.code_snippets.argo_runtime.extra_container_options import say_hello

    assert say_hello.runtime_options["argo_container_overrides"] != {}


def test_extra_template_options():
    from docs.code_snippets.argo_runtime.extra_template_options import say_hello

    assert say_hello.runtime_options["argo_template_overrides"] != {}


def test_extra_dag_template_options():
    from docs.code_snippets.argo_runtime.extra_dag_template_options import dag

    assert dag.runtime_options["argo_dag_template_overrides"] != {}


def test_extra_workflow_options():
    from docs.code_snippets.argo_runtime.extra_workflow_options import manifest

    assert "priority" in manifest["spec"]
    assert "nodeSelector" in manifest["spec"]


def test_extra_cron_workflow_options():
    from docs.code_snippets.argo_runtime.extra_cron_workflow_options import manifest

    assert "startingDeadlineSeconds" in manifest["spec"]
    assert "workflowMetadata" in manifest["spec"]


def test_generate_manifests():
    import os
    import tempfile

    import yaml

    from dagger import dsl
    from docs.code_snippets.argo_runtime.generate_manifests import dump_argo_manifests

    @dsl.task()
    def say_hello():
        print("Hello!")

    @dsl.DAG()
    def dag():
        say_hello()

    with tempfile.TemporaryDirectory() as tmp:
        output_path = os.path.join(tmp, "manifest.yaml")

        dump_argo_manifests(
            name="my-pipeline",
            filename="./my_filename.py",
            dag=dsl.build(dag),
            output_path=output_path,
        )

        with open(output_path, "r") as f:
            manifest = yaml.load(f, Loader=yaml.SafeLoader)
            assert manifest["kind"] == "Workflow"


def test_syntax_sugar():
    from docs.code_snippets.argo_runtime.syntax_sugar import argo_task

    @argo_task(
        cpu_units=4,
        memory_in_gigabytes=8.5,
        timeout_in_minutes=10,
        max_retries=5,
    )
    def my_task():
        pass

    assert my_task.runtime_options == {
        "argo_template_overrides": {
            "activeDeadlineSeconds": 600,
            "retryStrategy": {"limit": 5},
        },
        "argo_container_overrides": {
            "resources": {
                "requests": {
                    "cpu": "4",
                    "memory": "8.5Gi",
                },
            },
            "envFrom": [
                {"configMapRef": {"name": "default"}},
                {"secretRef": {"name": "default"}},
            ],
        },
    }
