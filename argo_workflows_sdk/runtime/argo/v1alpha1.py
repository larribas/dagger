import os
from typing import Dict, List, Optional

import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
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
                    "dag": {"tasks": [__node_task(node) for node in dag.nodes]},
                },
                *[
                    __node_template(
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


def __node_template(
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

    if node.outputs:
        if "outputs" not in manifest:
            manifest["outputs"] = {}

        output_parameters = {
            k: v for k, v in node.outputs.items() if isinstance(v, outputs.Param)
        }
        if output_parameters:
            manifest["outputs"]["parameters"] = __output_parameters(
                node_name, output_parameters
            )

    if node.inputs:
        if "inputs" not in manifest:
            manifest["inputs"] = {}

        input_parameters = {
            k: v
            for k, v in node.inputs.items()
            if isinstance(v, inputs.FromOutputParam)
        }
        if input_parameters:
            manifest["inputs"]["parameters"] = __input_parameters(
                input_parameters.keys()
            )

    return manifest


def __input_parameters(input_names: List[str]):
    return [
        {
            "name": input_name,
        }
        for input_name in input_names
    ]


def __output_parameters(node_name: str, params: Dict[str, outputs.Param]):
    return [
        {
            "name": output_name,
            "valueFrom": {
                "path": os.path.join(
                    DEFAULT_PARAM_FILE_PATH,
                    f"{node_name}.{output_name}.{param.serializer.extension}",
                )
            },
        }
        for output_name, param in params.items()
    ]


def __argument_parameters():
    return []


def __node_task(node: Node):
    task = {
        "name": node.name,
        "template": node.name,
    }

    if node.inputs:
        task["arguments"] = {}
