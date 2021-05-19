import os
from typing import Dict, List, Optional

import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
from argo_workflows_sdk.dag import DAG
from argo_workflows_sdk.node import Node

API_VERSION = "argoproj.io/v1alpha1"
DEFAULT_PARAM_FILE_PATH = "/tmp"


def workflow_manifest(
    dag: DAG,
    name: str,
    container_image: str,
    container_entrypoint_to_dag_cli: Optional[List[str]] = None,
    generate_name_from_prefix: bool = False,
    namespace: Optional[str] = None,
    annotations: Optional[Dict[str, str]] = None,
    labels: Optional[Dict[str, str]] = None,
    service_account: Optional[str] = None,
):
    container_entrypoint_to_dag_cli = container_entrypoint_to_dag_cli or []

    manifest = {
        "apiVersion": API_VERSION,
        "kind": "Workflow",
        "metadata": __workflow_metadata(
            name=name,
            generate_name_from_prefix=generate_name_from_prefix,
            namespace=namespace,
            annotations=annotations,
            labels=labels,
        ),
        "spec": {
            "entrypoint": "main",
            "templates": [
                {
                    "name": "main",
                    "dag": {
                        "tasks": [
                            __node_task(node_name, node)
                            for node_name, node in dag.nodes.items()
                        ]
                    },
                },
                *[
                    __node_template(
                        node_name,
                        node,
                        container_image=container_image,
                        container_command=container_entrypoint_to_dag_cli,
                    )
                    for node_name, node in dag.nodes.items()
                ],
            ],
        },
    }

    if namespace:
        manifest["metadata"]["namespace"] = namespace

    if service_account:
        manifest["spec"]["serviceAccountName"] = service_account

    return manifest


def __workflow_metadata(
    name: str,
    generate_name_from_prefix: bool,
    namespace: Optional[str] = None,
    annotations: Optional[Dict[str, str]] = None,
    labels: Optional[Dict[str, str]] = None,
):
    metadata = {}

    if generate_name_from_prefix:
        metadata["generateName"] = name
    else:
        metadata["name"] = name

    if annotations:
        metadata["annotations"] = annotations

    if labels:
        metadata["labels"] = labels

    return metadata


def __node_template(
    node_name: str,
    node: Node,
    container_image: str,
    container_command: List[str],
):
    manifest = {
        "name": node_name,
        "container": {
            "image": container_image,
            "command": container_command,
            "args": [f"--node-name={node_name}"],
        },
    }

    # if node.outputs:
    #     if "outputs" not in manifest:
    #         manifest["outputs"] = {}

    #     output_parameters = {
    #         k: v for k, v in node.outputs.items() if isinstance(v, outputs.Param)
    #     }
    #     if output_parameters:
    #         manifest["outputs"]["parameters"] = __output_parameters(
    #             node_name, output_parameters
    #         )

    # if node.inputs:
    #     if "inputs" not in manifest:
    #         manifest["inputs"] = {}

    #     input_parameters = {
    #         k: v
    #         for k, v in node.inputs.items()
    #         if isinstance(v, inputs.FromOutputParam)
    #     }
    #     if input_parameters:
    #         manifest["inputs"]["parameters"] = __input_parameters(
    #             input_parameters.keys()
    #         )

    return manifest


# def __input_parameters(input_names: List[str]):
#     return [
#         {
#             "name": input_name,
#         }
#         for input_name in input_names
#     ]


# def __output_parameters(node_name: str, params: Dict[str, outputs.Param]):
#     return [
#         {
#             "name": output_name,
#             "valueFrom": {
#                 "path": os.path.join(
#                     DEFAULT_PARAM_FILE_PATH,
#                     f"{node_name}.{output_name}.{param.serializer.extension}",
#                 )
#             },
#         }
#         for output_name, param in params.items()
#     ]


def __argument_parameters():
    return []


def __node_task(node_name: str, node: Node):
    task = {
        "name": node_name,
        "template": node_name,
    }

    if node.inputs:
        task["arguments"] = {}

    return task
