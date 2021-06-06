from typing import Dict

from examples.nested_dags import dag
from tests.examples.verification import verify_dag_works_with_all_runtimes


def validate_results(results: Dict[str, bytes]):
    assert results == {
        "album": b'{"name": "big dag", "tracks": ["recording of (hip hop song about love)", "recording of (hip hop song about loss)"]}'
    }


def test():
    verify_dag_works_with_all_runtimes(
        dag,
        params={
            "album_name": b'"big dag"',
            "style": b'"hip hop"',
        },
        validate_results=validate_results,
        argo_workflow_yaml_filename="nested_dags.yaml",
    )
