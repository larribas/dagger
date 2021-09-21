from dagger.runtime.argo.workflow import Workflow


def test__workflow__representation():
    container_image = "my-image:tag"
    extra_spec_options = {"extra": "options"}
    container_entrypoint = ["my", "entrypoint"]
    params = {"my": "params"}
    workflow = Workflow(
        container_image=container_image,
        container_entrypoint_to_dag_cli=container_entrypoint,
        params=params,
        extra_spec_options=extra_spec_options,
    )
    assert (
        repr(workflow)
        == f"Workflow(container_image=my-image:tag, container_entrypoint_to_dag_cli={repr(container_entrypoint)}, params={repr(params)}, extra_spec_options={repr(extra_spec_options)})"
    )


def test__workflow__eq():
    container_image = "my-image:tag"
    extra_spec_options = {"extra": "options"}
    container_entrypoint = ["my", "entrypoint"]
    params = {"my": "params"}
    workflow = Workflow(
        container_image=container_image,
        container_entrypoint_to_dag_cli=container_entrypoint,
        params=params,
        extra_spec_options=extra_spec_options,
    )
    assert workflow != Workflow(container_image=container_image)
    assert workflow == Workflow(
        container_image=container_image,
        container_entrypoint_to_dag_cli=container_entrypoint,
        params=params,
        extra_spec_options=extra_spec_options,
    )
