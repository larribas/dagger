import yaml

from argo_workflows_sdk.argo import as_workflow
from argo_workflows_sdk.examples.hello_world import dag


def test_hello_world():
    manifest = as_workflow(
        dag,
        name_prefix="hello-world-",
        namespace="my-namespace",
        service_account="my-namespace-workflows",
        container_image="my-image",
        container_command=["python", "argo_workflows_sdk"],
    )
    assert manifest == yaml.load("hello_world.yaml")
