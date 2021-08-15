from typing import Dict

from examples.static_parallelization import dag
from tests.examples.verification import verify_dag_works_with_all_runtimes


def validate_results(results: Dict[str, bytes]):
    assert results == {"sum": b"90"}


def test():
    verify_dag_works_with_all_runtimes(
        dag,
        params={
            "number": 2,
        },
        validate_results=validate_results,
        argo_workflow_yaml_filename="static_parallelization.yaml",
    )
