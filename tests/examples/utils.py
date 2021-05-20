from pathlib import Path
from typing import Callable, Dict, List

import yaml
from deepdiff import DeepDiff

from argo_workflows_sdk import DAG


def load_argo_manifest(filename: str):
    path = Path(__file__).parent / "argo" / filename
    with open(path, "r") as f:
        return yaml.safe_load(f)


def validate_example(
    dag: DAG,
    params: Dict[str, bytes],
    validate_results: Callable[[Dict[str, bytes]], None],
    argo_workflow_yaml_filename: str,
    container_entrypoint: List[str],
):
    validate_example_with_local_runtime(
        dag,
        params=params,
        validate_results=validate_results,
    )
    validate_example_with_cli_runtime(
        dag,
        params=params,
        validate_results=validate_results,
    )
    validate_example_with_argo_runtime(
        dag,
        params=params,
        expected_manifest=load_argo_manifest(argo_workflow_yaml_filename),
        container_entrypoint=container_entrypoint,
    )


def validate_example_with_local_runtime(
    dag: DAG,
    params: Dict[str, bytes],
    validate_results: Callable[[Dict[str, bytes]], None],
):
    from argo_workflows_sdk.runtime.local import invoke_dag

    results = invoke_dag(dag, params=params)
    validate_results(results)


def validate_example_with_cli_runtime(
    dag: DAG,
    params: Dict[str, bytes],
    validate_results: Callable[[Dict[str, bytes]], None],
):
    import itertools
    import os
    import tempfile

    from argo_workflows_sdk.runtime.cli import invoke

    with tempfile.TemporaryDirectory() as tmp:

        for param_name, param_value in params.items():
            with open(os.path.join(tmp, param_name), "wb") as f:
                f.write(param_value)

        invoke(
            dag,
            argv=itertools.chain(
                *[
                    ["--input", param_name, os.path.join(tmp, param_name)]
                    for param_name in params
                ],
                *[
                    ["--output", output_name, os.path.join(tmp, output_name)]
                    for output_name in dag.outputs
                ],
            ),
        )

        results = {}
        for output_name in dag.outputs:
            with open(os.path.join(tmp, output_name), "rb") as f:
                results[output_name] = f.read()

        validate_results(results)


def validate_example_with_argo_runtime(
    dag: DAG,
    params: Dict[str, bytes],
    expected_manifest: dict,
    container_entrypoint: List[str],
):
    from argo_workflows_sdk.runtime.argo import workflow_manifest

    generated_manifest = workflow_manifest(
        dag,
        params=params,
        name="some-name",
        container_image="local.registry/dagger",
        container_entrypoint_to_dag_cli=container_entrypoint,
    )

    diff = DeepDiff(
        expected_manifest["spec"],
        generated_manifest["spec"],
        ignore_order=True,
        view="tree",
    )

    print(f"Generated manifest is:\n{yaml.dump(generated_manifest)}")
    print(diff.pretty())
    assert not diff
