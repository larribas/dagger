from typing import Dict

from examples.passing_parameters import dag
from tests.examples.verification import verify_dag_works_with_all_runtimes


def validate_results(results: Dict[str, bytes]):
    assert results == {"number-doubled-and-squared": b"100"}


def test():
    verify_dag_works_with_all_runtimes(
        dag,
        params={
            "number": 5,
        },
        validate_results=validate_results,
        argo_workflow_yaml_filename="passing_parameters.yaml",
    )
