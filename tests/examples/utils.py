from pathlib import Path
from typing import Callable, Dict

import yaml
from deepdiff import DeepDiff

from argo_workflows_sdk import DAG


def load_yaml(name: str):
    example_yamls_path = Path(__file__).parent
    with open(f"{example_yamls_path}/{name}.yaml", "r") as f:
        return yaml.load(f)


def assert_deep_equal(a, b):
    """
    Compare the supplied data strcture against each other, and assert their equality.
    The motivation for this function is to produce a succint summary of the differences between both structures as an assertion message.
    """
    diff = DeepDiff(a, b, ignore_order=True)
    assert not diff


def validate_example(
    dag: DAG,
    params: Dict[str, bytes],
    validate_results: Callable[[Dict[str, bytes]], None],
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
