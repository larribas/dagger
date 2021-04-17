from typing import Callable, Dict

from argo_workflows_sdk import __version__
from argo_workflows_sdk.examples.hello_world import dag
from tests.examples.utils import validate_example


def test():
    validate_example(
        dag,
        params={},
        validate_results=lambda _results: None,
    )


# def test_on_argo():
#     # Given
#     manifest = as_workflow(
#         dag,
#         name_prefix="hello-world-",
#         container_image=f"argo_workflows_sdk:{__version__}",
#         container_dag_entrypoint=["hello-world"],
#     )
#     # Then
#     assert_deep_equal(
#         manifest,
#         load_yaml("argo/hello_world"),
#     )
