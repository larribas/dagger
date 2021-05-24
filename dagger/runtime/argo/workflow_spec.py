import itertools
import os
from typing import Any, Dict, List, Optional

import dagger.inputs as inputs
from dagger.dag import DAG, validate_parameters
from dagger.node import Node
from dagger.node import SupportedInputs as SupportedNodeInputs
from dagger.runtime.argo.errors import IncompatibilityError

ENTRYPOINT_TASK_NAME = "dag"
INPUT_PATH = "/tmp/inputs/"
OUTPUT_PATH = "/tmp/outputs/"


def workflow_spec(
    dag: DAG,
    container_image: str,
    params: Optional[Dict[str, bytes]] = None,
    container_entrypoint_to_dag_cli: Optional[List[str]] = None,
    service_account: Optional[str] = None,
) -> dict:
    """
    Returns a minimal representation of a WorkflowSpec for the supplied DAG and metadata
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflowspec
    """
    params = params or {}
    container_entrypoint_to_dag_cli = container_entrypoint_to_dag_cli or []

    validate_parameters(inputs=dag.inputs, params=params)

    spec = {
        "entrypoint": ENTRYPOINT_TASK_NAME,
        "templates": [
            dag_template(dag),
            *[
                node_template(
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
        spec["arguments"] = workflow_spec_arguments(params)

    if service_account:
        spec["serviceAccountName"] = service_account

    return spec


def workflow_spec_arguments(params: Dict[str, bytes]) -> dict:
    return {
        "artifacts": [
            {"name": param_name, "raw": {"data": param_value}}
            for param_name, param_value in params.items()
        ],
    }


def dag_template(
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
                dag_task(node_name, node) for node_name, node in dag.nodes.items()
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


def dag_task(node_name: str, node: Node) -> Dict[str, Any]:
    """
    Returns a minimal representation of a DAGTask for a specific node
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#dagtask
    """
    task: dict = {
        "name": node_name,
        "template": node_template_name(node_name),
    }

    dependencies = dag_task_dependencies(node)
    if dependencies:
        task["dependencies"] = dependencies

    if node.inputs:
        task["arguments"] = dag_task_arguments(node_name=node_name, node=node)

    return task


def dag_task_dependencies(node: Node) -> List[str]:
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


def dag_task_arguments(node_name: str, node: Node) -> dict:
    """
    Returns a minimal representation of an Arguments object,
    retrieving each of the node's inputs from the right source.
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#arguments
    """
    return {
        "artifacts": [
            {
                "name": input_name,
                "from": dag_task_argument_artifact_from(
                    node_name=node_name,
                    input_name=input_name,
                    input=input,
                ),
            }
            for input_name, input in node.inputs.items()
        ]
    }


def dag_task_argument_artifact_from(
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
            f"Whoops. Input '{input_name}' of node '{node_name}' is of type '{type(input).__name__}'. While this input type may be supported by the DAG, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
        )


def node_template(
    node_name: str,
    node: Node,
    container_image: str,
    container_command: List[str],
) -> dict:
    """
    Returns a minimal representation of a Template that executes a specific Node
    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """
    template: dict = {
        "name": node_template_name(node_name),
        "container": {
            "image": container_image,
            # We create a copy of the entrypoint to prevent the YAML library
            # from using anchors and aliases and make the resulting YAML more
            # explicit. It also allows any other postprocessor to change some
            # of the entrypoints without affecting the rest
            "command": container_command[:],
            "args": node_template_container_arguments(node_name, node),
        },
    }

    if node.inputs:
        template["inputs"] = node_template_inputs(node)

    if node.outputs:
        template["outputs"] = node_template_outputs(node)
        template["volumes"] = [{"name": "outputs", "emptyDir": {}}]
        template["container"]["volumeMounts"] = [
            {"name": "outputs", "mountPath": OUTPUT_PATH}
        ]

    return template


def node_template_inputs(node: Node) -> dict:
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


def node_template_outputs(node: Node) -> dict:
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


def node_template_container_arguments(node_name: str, node: Node) -> List[str]:
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


def node_template_name(node_name: str) -> str:
    """
    Generates a deterministic name for a task based on a node's name,
    guaranteeing that there will be no collisions with other template names.
    We do this to have the ability to define extra templates for other purposes
    (e.g. as a DAG entrypoint, or for use by other plugins and postprocessors)
    """
    return f"node-{node_name}"
