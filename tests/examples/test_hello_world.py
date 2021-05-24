from dagger.examples.hello_world import dag
from tests.examples.utils import validate_example


def test():
    validate_example(
        dag,
        params={},
        validate_results=lambda _results: None,
        argo_workflow_yaml_filename="hello_world.yaml",
        container_entrypoint=["hello-world"],
    )
