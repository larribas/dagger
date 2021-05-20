from typing import Dict

from argo_workflows_sdk.examples.passing_parameters import dag
from tests.examples.utils import validate_example


def validate_results(results: Dict[str, bytes]):
    assert results == {"number-doubled-and-squared": b"100"}


def test():
    validate_example(
        dag,
        params={
            "number": b"5",
        },
        validate_results=validate_results,
        argo_workflow_yaml_filename="passing_parameters.yaml",
        container_entrypoint=["passing-parameters"],
    )
