from argo_workflows_sdk.argo import as_workflow
from argo_workflows_sdk.examples.hello_world import dag
from tests.examples.test_utils import assert_deep_equal, load_yaml


def test_on_argo():
    # Given
    manifest = as_workflow(
        dag,
        name_prefix="hello-world-",
        namespace="default",
        service_account="default",
        container_image="argo_workflows_sdk",
        container_dag_entrypoint=["hello-world"],
    )
    # Then
    assert_deep_equal(
        manifest,
        load_yaml("argo/hello_world"),
    )
