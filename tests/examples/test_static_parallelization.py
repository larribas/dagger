from typing import Dict

from dagger.examples.static_parallelization import dag
from tests.examples.utils import validate_example


def validate_results(results: Dict[str, bytes]):
    assert results == {"sum": b"90"}


def test():
    validate_example(
        dag,
        params={
            "number": b"2",
        },
        validate_results=validate_results,
        argo_workflow_yaml_filename="static_parallelization.yaml",
        container_entrypoint=["static-parallelization"],
    )
