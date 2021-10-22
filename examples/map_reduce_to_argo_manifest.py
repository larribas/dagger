"""
# Map-Reduce to Argo Manifest.

In this task, we import the example from map_reduce_imperative, build a DAG with the DSL
and produce an Argo manifest with the function cron_workflow_manifest.

Then we save this file as a YAML in the examples folder.

"""

import yaml

from dagger import dsl
from dagger.runtime.argo import Cron, Metadata, Workflow, cron_workflow_manifest
from examples.map_reduce_imperative import map_reduce_pipeline

if __name__ == "__main__":
    """Define an Argo interface for the imported DAG, using the Argo runtime. Check
    the documentation to understand why this is relevant or necessary."""
    dag = dsl.build(map_reduce_pipeline)

    manifest = cron_workflow_manifest(
        dag,
        metadata=Metadata(
            name="map-reduce-pipeline",
        ),
        workflow=Workflow(
            container_image="my-docker-registry/my-image:my-tag",
            params={
                "exponent": 2,
            },
        ),
        cron=Cron(
            schedule="0 0 * * *",  # Every day at 00:00
        ),
    )
    with open("dummy_map_reduce.yaml", "w") as f:
        yaml.dump(manifest, f)
