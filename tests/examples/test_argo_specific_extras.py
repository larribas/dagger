from examples.argo_specific_extras import dag
from tests.examples.verification import verify_dag_works_with_all_runtimes


def test():
    verify_dag_works_with_all_runtimes(
        dag,
        params={},
        validate_results=lambda _results: None,
        argo_workflow_yaml_filename="argo_specific_extras.yaml",
    )
