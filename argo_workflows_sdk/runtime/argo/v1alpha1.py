import itertools
from typing import Dict, List, Optional

import argo_workflows_sdk.inputs as inputs
from argo_workflows_sdk.dag import DAG, validate_parameters
from argo_workflows_sdk.node import Node
from argo_workflows_sdk.node import SupportedInputs as SupportedNodeInputs
from argo_workflows_sdk.runtime.argo.errors import IncompatibilityError

API_VERSION = "argoproj.io/v1alpha1"
DEFAULT_PARAM_FILE_PATH = "/tmp"


# TODO: Modularize, document and link to the official documentation. The code is quite simple, but it may look daunting at first. We need to make it look inoffensive


def workflow_manifest(
    dag: DAG,
    name: str,
    container_image: str,
    params: Optional[Dict[str, bytes]] = None,
    container_entrypoint_to_dag_cli: Optional[List[str]] = None,
    generate_name_from_prefix: bool = False,
    namespace: Optional[str] = None,
    annotations: Optional[Dict[str, str]] = None,
    labels: Optional[Dict[str, str]] = None,
    service_account: Optional[str] = None,
):
    params = params or {}
    container_entrypoint_to_dag_cli = container_entrypoint_to_dag_cli or []

    validate_parameters(inputs=dag.inputs, params=params)

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
                __main_template(dag),
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

    if params:
        manifest["spec"]["arguments"] = {
            "artifacts": [
                {"name": param_name, "raw": {"data": param_value}}
                for param_name, param_value in params.items()
            ],
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


def __main_template(
    dag: DAG,
):
    template = {
        "name": "main",
        "dag": {
            "tasks": [
                __node_task(node_name, node) for node_name, node in dag.nodes.items()
            ]
        },
    }

    if dag.inputs:
        template["inputs"] = {
            "artifacts": [{"name": input_name} for input_name in dag.inputs.keys()]
        }

    if dag.outputs:
        template["outputs"] = {
            "artifacts": [
                {
                    "name": output_name,
                    "from": "{{"
                    + f"tasks.{output.node}.outputs.artifacts.{output.output}"
                    + "}}",
                }
                for output_name, output in dag.outputs.items()
            ]
        }

    return template


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
            "command": container_command[:],
            "args": ["--node-name", node_name],
        },
    }

    if node.inputs:
        manifest["inputs"] = {
            "artifacts": [
                {
                    "name": input_name,
                    "path": f"/tmp/inputs/{input_name}.{input.serializer.extension}",
                }
                for input_name, input in node.inputs.items()
            ]
        }
        manifest["container"]["args"] += itertools.chain(
            *[
                [
                    "--input",
                    input_name,
                    "{{" + f"inputs.artifacts.{input_name}.path" + "}}",
                ]
                for input_name in node.inputs.keys()
            ],
        )

    if node.outputs:
        manifest["outputs"] = {
            "artifacts": [
                {
                    "name": output_name,
                    "path": f"/tmp/outputs/{output_name}.{output.serializer.extension}",
                }
                for output_name, output in node.outputs.items()
            ]
        }
        manifest["container"]["args"] += itertools.chain(
            *[
                [
                    "--output",
                    output_name,
                    "{{" + f"outputs.artifacts.{output_name}.path" + "}}",
                ]
                for output_name in node.outputs.keys()
            ],
        )
        manifest["volumes"] = [
            {"name": "outputs", "emptyDir": {}},
        ]
        manifest["container"]["volumeMounts"] = [
            {"name": "outputs", "mountPath": "/tmp/outputs"},
        ]

    return manifest


def __argument_parameters():
    return []


def __node_task(node_name: str, node: Node):
    task = {
        "name": node_name,
        "template": node_name,
    }

    dependencies = [
        input.node
        for input in node.inputs.values()
        if isinstance(input, inputs.FromNodeOutput)
    ]

    if dependencies:
        task["dependencies"] = dependencies

    if node.inputs:
        task["arguments"] = {
            "artifacts": [
                {
                    "name": input_name,
                    "from": __node_task_artifact_from(
                        node_name=node_name,
                        input_name=input_name,
                        input=input,
                    ),
                }
                for input_name, input in node.inputs.items()
            ]
        }

    return task


def __node_task_artifact_from(
    node_name: str,
    input_name: str,
    input: SupportedNodeInputs,
):
    # TODO: Test this function in isolation
    if isinstance(input, inputs.FromParam):
        return "{{" + f"inputs.artifacts.{input_name}" + "}}"
    elif isinstance(input, inputs.FromNodeOutput):
        return "{{" + f"tasks.{input.node}.outputs.artifacts.{input.output}" + "}}"
    else:
        raise IncompatibilityError(
            f"Whoops. You have declared an input of type '{type(input)}' for node '{node_name}'. While the input is valid, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
        )
