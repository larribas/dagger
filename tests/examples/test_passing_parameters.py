from pprint import pp

from argo_workflows_sdk import __version__
from argo_workflows_sdk.argo import as_workflow
from argo_workflows_sdk.examples.passing_parameters import dag
from tests.examples.test_utils import assert_deep_equal, load_yaml


def test_on_argo():
    # Given
    manifest = as_workflow(
        dag,
        name_prefix="passing-parameters-",
        container_image=f"argo_workflows_sdk:{__version__}",
        container_dag_entrypoint=["passing-parameters"],
    )
    pp(manifest)
    # Then
    assert_deep_equal(
        manifest,
        load_yaml("argo/passing_parameters"),
    )
