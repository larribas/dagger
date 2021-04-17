from typing import Dict

from argo_workflows_sdk.examples.passing_parameters import dag
from tests.examples.utils import validate_example


def validate_results(results: Dict[str, bytes]):
    assert results == {"number-multiplied-by-2-and-squared": b"100"}


def test():
    validate_example(
        dag,
        params={
            "number": b"5",
        },
        validate_results=validate_results,
    )


# def test_on_argo():
#     # Given
#     manifest = as_workflow(
#         dag,
#         name_prefix="passing-parameters-",
#         container_image=f"argo_workflows_sdk:{__version__}",
#         container_dag_entrypoint=["passing-parameters"],
#     )
#     pp(manifest)
#     # Then
#     assert_deep_equal(
#         manifest,
#         load_yaml("argo/passing_parameters"),
#     )
