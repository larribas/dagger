import yaml

from dagger import DAG
from dagger.runtime.argo import Metadata, Workflow, workflow_manifest


def dump_argo_manifests(
    name: str,
    filename: str,
    dag: DAG,
    output_path: str,
):
    manifest = workflow_manifest(
        dag,
        metadata=Metadata(
            name=name,
        ),
        workflow=Workflow(
            container_image="my_image",
            container_entrypoint_to_dag_cli=[
                "python",
                filename,
            ],
        ),
    )

    with open(output_path, "w") as f:
        yaml.dump(manifest, f, Dumper=yaml.SafeDumper)
