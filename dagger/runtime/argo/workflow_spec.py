"""Generate Workflow specifications."""
import itertools
import os
from typing import Dict, List, Optional, Union

import dagger.inputs as inputs
from dagger.dag import DAG, validate_parameters
from dagger.node import Node
from dagger.node import SupportedInputs as SupportedNodeInputs
from dagger.runtime.argo.errors import IncompatibilityError

BASE_DAG_NAME = "dag"
INPUT_PATH = "/tmp/inputs/"
OUTPUT_PATH = "/tmp/outputs/"


def workflow_spec(
    dag: DAG,
    container_image: str,
    container_entrypoint_to_dag_cli: Optional[List[str]] = None,
    params: Optional[Dict[str, bytes]] = None,
    service_account: Optional[str] = None,
) -> dict:
    """
    Return a minimal representation of a WorkflowSpec for the supplied DAG and metadata.

    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflowspec


    Parameters
    ----------
    dag : DAG
        The DAG to generate the spec for

    container_image : str
        The URI to the container image Argo will use for each of the steps

    container_entrypoint_to_dag_cli : List of str
        The container's entrypoint.
        It must run the specified DAG via the CLI runtime.
        Check the examples and documentation to understand better how to containerize your projects and expose your DAGs through the container.

    params : Dictionary of str -> bytes
        Parameters to inject to the DAG.
        They must match the inputs the DAG expects.

    service_account : str
        The Kubernetes service account
    """
    params = params or {}
    container_entrypoint_to_dag_cli = container_entrypoint_to_dag_cli or []

    validate_parameters(inputs=dag.inputs, params=params)

    spec = {
        "entrypoint": BASE_DAG_NAME,
        "templates": _templates(
            dag_or_node=dag,
            container_image=container_image,
            container_command=container_entrypoint_to_dag_cli,
        ),
    }

    if params:
        spec["arguments"] = _workflow_spec_arguments(params)

    if service_account:
        spec["serviceAccountName"] = service_account

    return spec


def _workflow_spec_arguments(params: Dict[str, bytes]) -> dict:
    return {
        "artifacts": [
            {"name": param_name, "raw": {"data": param_value}}
            for param_name, param_value in params.items()
        ],
    }


def _templates(
    dag_or_node: Union[DAG, Node],
    container_image: str,
    container_command: List[str],
    address: List[str] = None,
) -> List[dict]:
    """
    Return a list of Template resources for all the sub-DAGs and sub-nodes.

    This function is prepared to be used recursively.

    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """
    address = address or []

    if isinstance(dag_or_node, Node):
        return [
            _node_template(
                node=dag_or_node,
                address=address,
                container_image=container_image,
                container_command=container_command,
            )
        ]

    return list(
        itertools.chain(
            [
                _dag_template(
                    dag=dag_or_node,
                    address=address,
                )
            ],
            *[
                _templates(
                    dag_or_node=node,
                    address=address + [node_name],
                    container_image=container_image,
                    container_command=container_command,
                )
                for node_name, node in dag_or_node.nodes.items()
            ],
        )
    )


def _dag_template(
    dag: DAG,
    address: List[str] = None,
) -> dict:
    """
    Return a minimal representation of a Template that uses 'tasks' to orchestrate the supplied DAG.

    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """
    address = address or []

    template = {
        "name": _template_name(address),
        "dag": {
            "tasks": [
                _dag_task(
                    dag_or_node=node,
                    node_address=address + [node_name],
                )
                for node_name, node in dag.nodes.items()
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


def _dag_task(dag_or_node: Union[DAG, Node], node_address: List[str]) -> dict:
    """
    Return a minimal representation of a DAGTask for a specific node.

    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#dagtask
    """
    task: dict = {
        "name": node_address[-1],
        "template": _template_name(node_address),
    }

    dependencies = _dag_task_dependencies(dag_or_node)
    if dependencies:
        task["dependencies"] = dependencies

    if dag_or_node.inputs:
        task["arguments"] = _dag_task_arguments(
            dag_or_node=dag_or_node, node_address=node_address
        )

    return task


def _dag_task_dependencies(dag_or_node: Union[DAG, Node]) -> List[str]:
    """
    Return a list of dependencies for the current node.

    Dependencies are based on the inputs of the node. If one of the node's inputs depends on the output of another node N, we add N to the list of dependencies.
    """
    return [
        input.node
        for input in dag_or_node.inputs.values()
        if isinstance(input, inputs.FromNodeOutput)
    ]


def _dag_task_arguments(dag_or_node: Union[DAG, Node], node_address: List[str]) -> dict:
    """
    Return a minimal representation of an Arguments object, retrieving each of the node's inputs from the right source.

    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#arguments
    """
    return {
        "artifacts": [
            {
                "name": input_name,
                "from": _dag_task_argument_artifact_from(
                    node_address=node_address,
                    input_name=input_name,
                    input=input,
                ),
            }
            for input_name, input in dag_or_node.inputs.items()
        ]
    }


def _dag_task_argument_artifact_from(
    node_address: List[str],
    input_name: str,
    input: SupportedNodeInputs,
) -> str:
    """
    Return a pointer to the source of a specific artifact, based on the type of each input, and using Argo's workflow variables.

    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/variables.md
    """
    if isinstance(input, inputs.FromParam):
        return "{{" + f"inputs.artifacts.{input_name}" + "}}"
    elif isinstance(input, inputs.FromNodeOutput):
        return "{{" + f"tasks.{input.node}.outputs.artifacts.{input.output}" + "}}"
    else:
        node_name = ".".join(node_address)
        raise IncompatibilityError(
            f"Whoops. Input '{input_name}' of node '{node_name}' is of type '{type(input).__name__}'. While this input type may be supported by the DAG, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
        )


def _node_template(
    node: Node,
    address: List[str],
    container_image: str,
    container_command: List[str],
) -> dict:
    """
    Return a minimal representation of a Template that executes a specific Node.

    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """
    template: dict = {
        "name": _template_name(address),
        "container": {
            "image": container_image,
            # We create a copy of the entrypoint to prevent the YAML library
            # from using anchors and aliases and make the resulting YAML more
            # explicit. It also allows any other postprocessor to change some
            # of the entrypoints without affecting the rest
            "command": container_command[:],
            "args": _node_template_container_arguments(node=node, address=address),
        },
    }

    if node.inputs:
        template["inputs"] = _node_template_inputs(node)

    if node.outputs:
        template["outputs"] = _node_template_outputs(node)
        template["volumes"] = [{"name": "outputs", "emptyDir": {}}]
        template["container"]["volumeMounts"] = [
            {"name": "outputs", "mountPath": OUTPUT_PATH}
        ]

    return template


def _node_template_inputs(node: Node) -> dict:
    """
    Return a minimal representation of an Inputs object, mounting all the inputs a node needs as artifacts in a given path.

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


def _node_template_outputs(node: Node) -> dict:
    """
    Return a minimal representation of an Outputs object, pointing all the outputs a node produces to artifacts in a given path.

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


def _node_template_container_arguments(node: Node, address: List[str]) -> List[str]:
    """
    Return a list of arguments to supply to the CLI runtime to run a specific DAG node with a set of inputs and outputs mounted as artifacts.

    https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#outputs
    """
    return list(
        itertools.chain(
            *[
                ["--node-name", ".".join(address)],
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


def _template_name(address: List[str]) -> str:
    """
    Generate a template name from a node address.

    If the name is None, we return the default base DAG name.
    If the name is defined, we prefix it with the base DAG name. We do this to guarantee all names are unique.
    """
    return "-".join([BASE_DAG_NAME] + address)
