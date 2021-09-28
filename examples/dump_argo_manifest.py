"""CLI utility to dump the manifest for a specific example."""

from typing import List

import yaml

from dagger import DAG
from dagger.runtime.argo import Metadata, Workflow, workflow_manifest


def dump_argo_manifest(dag: DAG, example_name: str):
    """Generate a workflow manifest from the supplied DAG and dump it as a YAML file into tests/examples/argo/."""
    manifest = _generate_manifest(
        dag,
        name=example_name.replace("_", "-"),
        entrypoint=["python", f"examples/{example_name}.py"],
    )

    with open(f"tests/examples/argo/{example_name}.yaml", "w") as f:
        yaml.safe_dump(manifest, f)


def _generate_manifest(dag: DAG, name: str, entrypoint: List[str]):
    metadata = Metadata(
        name=name,
        generate_name_from_prefix=True,
    )
    workflow = Workflow(
        container_image="local.registry/dagger",
        container_entrypoint_to_dag_cli=entrypoint,
    )
    return workflow_manifest(dag, metadata=metadata, workflow=workflow)
