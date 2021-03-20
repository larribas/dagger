import os
from typing import List, Optional

from argo_workflows_sdk import DAG, Node

API_VERSION = "argoproj.io/v1alpha1"
DEFAULT_PARAM_FILE_PATH = "/tmp"


def as_workflow(
    dag: DAG,
    name_prefix: str,
    container_image: str,
    container_dag_entrypoint: List[str],
    namespace: Optional[str] = None,
    service_account: Optional[str] = None,
):
    manifest = {
        "apiVersion": API_VERSION,
        "kind": "Workflow",
        "metadata": {
            "generateName": name_prefix,
        },
        "spec": {
            "entrypoint": "main",
            "templates": [
                {
                    "name": "main",
                    "dag": {"tasks": [_node_task(node) for node in dag.nodes]},
                },
                *[
                    _node_template(
                        node,
                        container_image=container_image,
                        container_command=container_dag_entrypoint,
                    )
                    for node in dag.nodes
                ],
            ],
        },
    }

    if namespace:
        manifest["metadata"]["namespace"] = namespace

    if service_account:
        manifest["spec"]["serviceAccountName"] = service_account

    return manifest


def _node_template(
    node: Node,
    container_image: str,
    container_command: List[str],
):
    manifest = {
        "name": node.name,
        "container": {
            "image": container_image,
            "command": container_command,
            "args": [f"--node-name={node.name}"],
        },
    }

    if node.output_params:
        if "outputs" not in manifest:
            manifest["outputs"] = {}

        manifest["outputs"]["parameters"] = [
            {
                "name": output_name,
                "valueFrom": {
                    "path": os.path.join(
                        DEFAULT_PARAM_FILE_PATH,
                        f"{node.name}.{output_serializer.extension}",
                    )
                },
            }
            for output_name, output_serializer in node.output_params.items()
        ]

    if node.input_params:
        if "inputs" not in manifest:
            manifest["inputs"] = {}

        manifest["inputs"]["parameters"] = [
            {
                "name": input_name,
            }
            for input_name in node.output_params.keys()
        ]

    return manifest


def _node_task(node: Node):
    return {
        "name": node.name,
        "template": node.name,
    }
