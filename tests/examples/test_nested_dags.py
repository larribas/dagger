from typing import Dict

from dagger.examples.nested_dags import dag
from tests.examples.utils import validate_example


def validate_results(results: Dict[str, bytes]):
    assert results == {
        "album": b'{"name": "big dag", "tracks": ["recording of (hip hop song about love)", "recording of (hip hop song about loss)"]}'
    }


def test():
    validate_example(
        dag,
        params={
            "album_name": b'"big dag"',
            "style": b'"hip hop"',
        },
        validate_results=validate_results,
        argo_workflow_yaml_filename="nested_dags.yaml",
        container_entrypoint=["nested-dags"],
    )
