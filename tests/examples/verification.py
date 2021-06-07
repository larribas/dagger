"""Verify examples work with the provided runtimes."""

from pathlib import Path
from typing import Callable, Mapping

import yaml
from deepdiff import DeepDiff

from dagger import DAG


def verify_dag_works_with_all_runtimes(
    dag: DAG,
    params: Mapping[str, bytes],
    validate_results: Callable[[Mapping[str, bytes]], None],
    argo_workflow_yaml_filename: str,
):
    """
    Run/Compile the DAG using all available runtimes and verifies its behavior or expectations.

    Parameters
    ----------
    dag
        The DAG to test

    params
        A mapping of parameter names to values

    validate_results
        A function validating the outputs of the DAG

    argo_workflow_yaml_filename
        The filename of the YAML manifest we expect the Argo runtime to produce
    """
    verify_dag_works_with_local_runtime(
        dag,
        params=params,
        validate_results=validate_results,
    )
    verify_dag_works_with_cli_runtime(
        dag,
        params=params,
        validate_results=validate_results,
    )
    verify_dag_matches_expected_manifest_when_using_argo_runtime(
        dag,
        params=params,
        expected_manifest=_load_argo_manifest(argo_workflow_yaml_filename),
    )


def verify_dag_works_with_local_runtime(
    dag: DAG,
    params: Mapping[str, bytes],
    validate_results: Callable[[Mapping[str, bytes]], None],
):
    """
    Run the DAG using the local runtime, and calls the supplied function to validate the results.

    Parameters
    ----------
    dag
        The DAG to test

    params
        A mapping of parameter names to values

    validate_results
        A function validating the outputs of the DAG
    """
    from dagger.runtime.local import invoke_dag

    results = invoke_dag(dag, params=params)
    validate_results(results)


def verify_dag_works_with_cli_runtime(
    dag: DAG,
    params: Mapping[str, bytes],
    validate_results: Callable[[Mapping[str, bytes]], None],
):
    """
    Run the DAG using the CLI runtime, and calls the supplied function to validate the results.

    Parameters
    ----------
    dag
        The DAG to test

    params
        A mapping of parameter names to values

    validate_results
        A function validating the outputs of the DAG
    """
    import itertools
    import os
    import tempfile

    from dagger.runtime.cli import invoke

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


def verify_dag_matches_expected_manifest_when_using_argo_runtime(
    dag: DAG,
    params: Mapping[str, bytes],
    expected_manifest: dict,
):
    """
    Compile the DAG into Argo CRD manifests using the Argo runtime, and validates the results against a pre-compiled version that has been tested previously.

    Parameters
    ----------
    dag
        The DAG to test

    params
        A mapping of parameter names to values

    expected_manifest
        An Argo CRD representing a workflow that runs the supplied DAG.
        We only check the 'spec' section of the workflow. Therefore, metadata, API versioning or scheduling options do not have any effect on the result of calling this function.
    """
    import dagger.runtime.argo as argo

    try:
        container_entrypoint = [
            template["container"].get("command", [])
            for template in expected_manifest["spec"]["templates"]
            if "container" in template
        ][0]
    except KeyError:
        raise ValueError(
            "The argo manifest you pointed to does not contain any template that uses a container. We expect DAGs to have, at least, one task, and the task to be executed through a container. There may be a mistake with the file you created. Please check other examples for reference."
        )

    generated_manifest = argo.workflow_manifest(
        dag,
        params=params,
        metadata=argo.Metadata(name="some-name"),
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


def _load_argo_manifest(filename: str):
    path = Path(__file__).parent / "argo" / filename
    with open(path, "r") as f:
        return yaml.safe_load(f)
