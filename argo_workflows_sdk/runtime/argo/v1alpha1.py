import itertools
import os
from typing import Dict, List, Optional

import argo_workflows_sdk.inputs as inputs
from argo_workflows_sdk.dag import DAG, validate_parameters
from argo_workflows_sdk.node import Node
from argo_workflows_sdk.node import SupportedInputs as SupportedNodeInputs
from argo_workflows_sdk.runtime.argo.errors import IncompatibilityError

API_VERSION = "argoproj.io/v1alpha1"
ENTRYPOINT_TASK_NAME = "dag"
INPUT_PATH = "/tmp/inputs/"
OUTPUT_PATH = "/tmp/outputs/"


# TODO: Support CronWorkflow


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
) -> dict:
    """
    Returns a minimal representation of a Workflow to execute the supplied DAG
    with the specified metadata
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflow
    """

    params = params or {}
    container_entrypoint_to_dag_cli = container_entrypoint_to_dag_cli or []

    validate_parameters(inputs=dag.inputs, params=params)

    return {
        "apiVersion": API_VERSION,
        "kind": "Workflow",
        "metadata": __object_meta(
            name=name,
            generate_name_from_prefix=generate_name_from_prefix,
            namespace=namespace,
            annotations=annotations,
            labels=labels,
        ),
        "spec": __workflow_spec(
            dag=dag,
            params=params,
            container_image=container_image,
            container_entrypoint_to_dag_cli=container_entrypoint_to_dag_cli,
            service_account=service_account,
        ),
    }


def __object_meta(
    name: str,
    generate_name_from_prefix: bool,
    namespace: Optional[str] = None,
    annotations: Optional[Dict[str, str]] = None,
    labels: Optional[Dict[str, str]] = None,
) -> dict:
    """
    Returns a minimal representation of an ObjectMeta with the supplied information
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#objectmeta
    """
    metadata = {}

    if generate_name_from_prefix:
        metadata["generateName"] = name
    else:
        metadata["name"] = name

    if annotations:
        metadata["annotations"] = annotations

    if labels:
        metadata["labels"] = labels

    if namespace:
        metadata["namespace"] = namespace

    return metadata


def __workflow_spec(
    dag: DAG,
    container_image: str,
    params: Dict[str, bytes],
    container_entrypoint_to_dag_cli: List[str],
    service_account: Optional[str] = None,
) -> dict:
    """
    Returns a minimal representation of a WorkflowSpec for the supplied DAG and metadata
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflowspec
    """
    spec = {
        "entrypoint": ENTRYPOINT_TASK_NAME,
        "templates": [
            __dag_template(dag),
            *[
                __node_template(
                    node_name=node_name,
                    node=node,
                    container_image=container_image,
                    container_command=container_entrypoint_to_dag_cli,
                )
                for node_name, node in dag.nodes.items()
            ],
        ],
    }

    if params:
        spec["arguments"] = __workflow_spec_arguments(params)

    if service_account:
        spec["serviceAccountName"] = service_account

    return spec


def __workflow_spec_arguments(params: Dict[str, bytes]) -> dict:
    return {
        "artifacts": [
            {"name": param_name, "raw": {"data": param_value}}
            for param_name, param_value in params.items()
        ],
    }


def __dag_template(
    dag: DAG,
) -> dict:
    """
    Returns a minimal representation of a Template that uses 'tasks' to orchestrate
    the supplied DAG
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """
    template = {
        "name": ENTRYPOINT_TASK_NAME,
        "dag": {
            "tasks": [
                __dag_task(node_name, node) for node_name, node in dag.nodes.items()
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


def __dag_task(node_name: str, node: Node) -> dict:
    """
    Returns a minimal representation of a DAGTask for a specific node
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#dagtask
    """
    task = {
        "name": node_name,
        "template": __node_template_name(node_name),
    }

    dependencies = __dag_task_dependencies(node)
    if dependencies:
        task["dependencies"] = dependencies

    if node.inputs:
        task["arguments"] = __dag_task_arguments(node_name=node_name, node=node)

    return task


def __dag_task_dependencies(node: Node) -> List[str]:
    """
    Returns a list of dependencies for the current node.
    Dependencies are based on the inputs of the node. If one of the node's inputs
    depends on the output of another node N, we add N to the list of dependencies.
    """
    return [
        input.node
        for input in node.inputs.values()
        if isinstance(input, inputs.FromNodeOutput)
    ]


def __dag_task_arguments(node_name: str, node: Node) -> dict:
    """
    Returns a minimal representation of an Arguments object,
    retrieving each of the node's inputs from the right source.
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#arguments
    """
    return {
        "artifacts": [
            {
                "name": input_name,
                "from": __dag_task_argument_artifact_from(
                    node_name=node_name,
                    input_name=input_name,
                    input=input,
                ),
            }
            for input_name, input in node.inputs.items()
        ]
    }


def __dag_task_argument_artifact_from(
    node_name: str,
    input_name: str,
    input: SupportedNodeInputs,
) -> str:
    """
    Returns a pointer to the source of a specific artifact:
     - Based on the type of each input.
     - Using Argo's workflow variables: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/variables.md
    """
    if isinstance(input, inputs.FromParam):
        return "{{" + f"inputs.artifacts.{input_name}" + "}}"
    elif isinstance(input, inputs.FromNodeOutput):
        return "{{" + f"tasks.{input.node}.outputs.artifacts.{input.output}" + "}}"
    else:
        raise IncompatibilityError(
            f"Whoops. You have declared an input of type '{type(input)}' for node '{node_name}'. While the input is valid, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
        )


def __node_template(
    node_name: str,
    node: Node,
    container_image: str,
    container_command: List[str],
) -> dict:
    """
    Returns a minimal representation of a Template that executes a specific Node
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """
    template = {
        "name": __node_template_name(node_name),
        "container": {
            "image": container_image,
            # We create a copy of the entrypoint to prevent the YAML library
            # from using anchors and aliases and make the resulting YAML more
            # explicit. It also allows any other postprocessor to change some
            # of the entrypoints without affecting the rest
            "command": container_command[:],
            "args": __node_template_container_arguments(node_name, node),
        },
    }

    if node.inputs:
        template["inputs"] = __node_template_inputs(node)

    if node.outputs:
        template["outputs"] = __node_template_outputs(node)
        template["volumes"] = [{"name": "outputs", "emptyDir": {}}]
        template["container"]["volumeMounts"] = [
            {"name": "outputs", "mountPath": OUTPUT_PATH}
        ]

    return template


def __node_template_inputs(node: Node) -> dict:
    """
    Returns a minimal representation of an Inputs object,
    mounting all the inputs a node needs as artifacts in a given path
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#inputs
    """
    return {
        "artifacts": [
            {
                "name": input_name,
                "path": os.path.join(
                    INPUT_PATH, f"{input_name}.{input.serializer.extension}"
                ),
            }
            for input_name, input in node.inputs.items()
        ]
    }


def __node_template_outputs(node: Node) -> dict:
    """
    Returns a minimal representation of an Outputs object,
    pointing all the outputs a node produces to artifacts in a given path
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#outputs
    """
    return {
        "artifacts": [
            {
                "name": output_name,
                "path": os.path.join(
                    OUTPUT_PATH, f"{output_name}.{output.serializer.extension}"
                ),
            }
            for output_name, output in node.outputs.items()
        ]
    }


def __node_template_container_arguments(node_name: str, node: Node) -> List[str]:
    """
    Returns a list of arguments to supply to the CLI runtime to run a specific DAG node
    with a set of inputs and outputs mounted as artifacts
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#outputs
    """
    return list(
        itertools.chain(
            *[
                ["--node-name", node_name],
                *[
                    [
                        "--input",
                        input_name,
                        "{{" + f"inputs.artifacts.{input_name}.path" + "}}",
                    ]
                    for input_name in node.inputs.keys()
                ],
                *[
                    [
                        "--output",
                        output_name,
                        "{{" + f"outputs.artifacts.{output_name}.path" + "}}",
                    ]
                    for output_name in node.outputs.keys()
                ],
            ]
        )
    )


def __node_template_name(node_name: str) -> str:
    """
    Generates a deterministic name for a task based on a node's name,
    guaranteeing that there will be no collisions with other template names.
    We do this to have the ability to define extra templates for other purposes
    (e.g. as a DAG entrypoint, or for use by other plugins and postprocessors)
    """
    return f"node-{node_name}"
