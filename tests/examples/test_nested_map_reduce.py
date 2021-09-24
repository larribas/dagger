from typing import Dict

from examples.nested_map_reduce import dag
from tests.examples.verification import verify_dag_works_with_all_runtimes


def validate_results(results: Dict[str, bytes]):
    assert results == {"sum": 90}


def test():
    verify_dag_works_with_all_runtimes(
        dag,
        params={
            "multiplier": 3,
            "parallel_steps": 5,
        },
        validate_results=validate_results,
        argo_workflow_yaml_filename="nested_map_reduce.yaml",
    )
