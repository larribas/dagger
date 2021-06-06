"""Generate Workflow specifications."""
import itertools
import os
from typing import Any, Dict, List, Mapping, Optional

from dagger.dag import DAG, Node, validate_parameters
from dagger.input import FromNodeOutput, FromParam
from dagger.runtime.argo.errors import IncompatibilityError
from dagger.task import SupportedInputs as SupportedTaskInputs
from dagger.task import Task

BASE_DAG_NAME = "dag"
INPUT_PATH = "/tmp/inputs/"
OUTPUT_PATH = "/tmp/outputs/"


def workflow_spec(
    dag: DAG,
    container_image: str,
    container_entrypoint_to_dag_cli: Optional[List[str]] = None,
    params: Optional[Mapping[str, bytes]] = None,
    service_account: Optional[str] = None,
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a WorkflowSpec for the supplied DAG and metadata.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflowspec


    Parameters
    ----------
    dag
        The DAG to generate the spec for

    container_image
        The URI to the container image Argo will use for each of the tasks

    container_entrypoint_to_dag_cli
        The container's entrypoint.
        It must run the specified DAG via the CLI runtime.
        Check the examples and documentation to understand better how to containerize your projects and expose your DAGs through the container.

    params
        Parameters to inject to the DAG.
        They must match the inputs the DAG expects.

    service_account
        The Kubernetes service account
    """
    params = params or {}
    container_entrypoint_to_dag_cli = container_entrypoint_to_dag_cli or []

    validate_parameters(inputs=dag.inputs, params=params)

    spec = {
        "entrypoint": BASE_DAG_NAME,
        "templates": _templates(
            node=dag,
            container_image=container_image,
            container_command=container_entrypoint_to_dag_cli,
        ),
    }

    if params:
        spec["arguments"] = _workflow_spec_arguments(params)

    if service_account:
        spec["serviceAccountName"] = service_account

    return spec


def _workflow_spec_arguments(params: Mapping[str, bytes]) -> Mapping[str, Any]:
    return {
        "artifacts": [
            {"name": param_name, "raw": {"data": params[param_name]}}
            for param_name in params
        ],
    }


def _templates(
    node: Node,
    container_image: str,
    container_command: List[str],
    address: List[str] = None,
) -> List[Mapping[str, Any]]:
    """
    Return a list of Template resources for all the sub-DAGs and sub-nodes.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template

    This function is prepared to be used recursively.


    Parameters
    ----------
    node
        The node to generate templates for.

    container_image
        The URI to the container image Argo will use for each of the tasks.

    container_command
        The container's entrypoint.

    address
        A list of node names that point to this node.
        For instance, in a nested DAG, the address ["outer", "inner"] would point to a node named "inner", defined inside of a DAG named "outer", defined inside of the root DAG.
        If not specified, it defaults to an empty list.
        The address should only be empty for the root node of the DAG.


    Returns
    -------
    A list of template specifications.
    """
    address = address or []

    if isinstance(node, Task):
        task = node
        return [
            _task_template(
                task=task,
                address=address,
                container_image=container_image,
                container_command=container_command,
            )
        ]
    elif isinstance(node, DAG):
        dag = node
        return list(
            itertools.chain(
                [
                    _dag_template(
                        dag=dag,
                        address=address,
                    )
                ],
                *[
                    _templates(
                        node=dag.nodes[node_name],
                        address=address + [node_name],
                        container_image=container_image,
                        container_command=container_command,
                    )
                    for node_name in dag.nodes
                ],
            )
        )
    else:
        # TODO: Test this scenario
        human_readable_node_address = (
            f"Node {'.'.join(address)}" if address else "This node"
        )
        raise IncompatibilityError(
            f"Whoops. Node '{human_readable_node_address}' is of type '{type(node).__name__}'. While this node type may be supported by the DAG, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
        )


def _dag_template(
    dag: DAG,
    address: List[str] = None,
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a Template that uses 'tasks' to orchestrate the supplied DAG.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """
    address = address or []

    template = {
        "name": _template_name(address),
        "dag": {
            "tasks": [
                _dag_task(
                    node=dag.nodes[node_name],
                    node_address=address + [node_name],
                )
                for node_name in dag.nodes
            ]
        },
    }

    if dag.inputs:
        template["inputs"] = {
            "artifacts": [{"name": input_name} for input_name in dag.inputs]
        }

    if dag.outputs:
        template["outputs"] = {
            "artifacts": [
                {
                    "name": output_name,
                    "from": "{{"
                    + f"tasks.{dag.outputs[output_name].node}.outputs.artifacts.{dag.outputs[output_name].output}"
                    + "}}",
                }
                for output_name in dag.outputs
            ]
        }

    return template


def _dag_task(
    node: Node,
    node_address: List[str],
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a DAGTask for a specific node.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#dagtask
    """
    dag_task: Dict[str, Any] = {
        "name": node_address[-1],
        "template": _template_name(node_address),
    }

    dependencies = _dag_task_dependencies(node)
    if dependencies:
        dag_task["dependencies"] = dependencies

    if node.inputs:
        dag_task["arguments"] = _dag_task_arguments(
            node=node,
            node_address=node_address,
        )

    return dag_task


def _dag_task_dependencies(node: Node) -> List[str]:
    """
    Return a list of dependencies for the current node.

    Dependencies are based on the inputs of the node. If one of the node's inputs depends on the output of another node N, we add N to the list of dependencies.
    """
    return [
        input_from_node_output.node
        for input_from_node_output in node.inputs.values()
        if isinstance(input_from_node_output, FromNodeOutput)
    ]


def _dag_task_arguments(
    node: Node,
    node_address: List[str],
) -> Mapping[str, Any]:
    """
    Return a minimal representation of an Arguments object, retrieving each of the node's inputs from the right source.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#arguments
    """
    return {
        "artifacts": [
            {
                "name": input_name,
                "from": _dag_task_argument_artifact_from(
                    node_address=node_address,
                    input_name=input_name,
                    input=node.inputs[input_name],
                ),
            }
            for input_name in node.inputs
        ]
    }


def _dag_task_argument_artifact_from(
    node_address: List[str],
    input_name: str,
    input: SupportedTaskInputs,
) -> str:
    """
    Return a pointer to the source of a specific artifact, based on the type of each input, and using Argo's workflow variables.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/variables.md
    """
    if isinstance(input, FromParam):
        return "{{" + f"inputs.artifacts.{input_name}" + "}}"
    elif isinstance(input, FromNodeOutput):
        return "{{" + f"tasks.{input.node}.outputs.artifacts.{input.output}" + "}}"
    else:
        node_name = ".".join(node_address)
        raise IncompatibilityError(
            f"Whoops. Input '{input_name}' of node '{node_name}' is of type '{type(input).__name__}'. While this input type may be supported by the DAG, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
        )


def _task_template(
    task: Task,
    address: List[str],
    container_image: str,
    container_command: List[str],
) -> Mapping[str, Any]:
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
            "args": _task_template_container_arguments(task=task, address=address),
        },
    }

    if task.inputs:
        template["inputs"] = _task_template_inputs(task)

    if task.outputs:
        template["outputs"] = _task_template_outputs(task)
        template["volumes"] = [{"name": "outputs", "emptyDir": {}}]
        template["container"]["volumeMounts"] = [
            {"name": "outputs", "mountPath": OUTPUT_PATH}
        ]

    return template


def _task_template_inputs(task: Task) -> Mapping[str, Any]:
    """
    Return a minimal representation of an Inputs object, mounting all the inputs a node needs as artifacts in a given path.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#inputs
    """
    return {
        "artifacts": [
            {
                "name": input_name,
                "path": os.path.join(
                    INPUT_PATH,
                    f"{input_name}.{task.inputs[input_name].serializer.extension}",
                ),
            }
            for input_name in task.inputs
        ]
    }


def _task_template_outputs(task: Task) -> Mapping[str, Any]:
    """
    Return a minimal representation of an Outputs object, pointing all the outputs a node produces to artifacts in a given path.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#outputs
    """
    return {
        "artifacts": [
            {
                "name": output_name,
                "path": os.path.join(
                    OUTPUT_PATH,
                    f"{output_name}.{task.outputs[output_name].serializer.extension}",
                ),
            }
            for output_name in task.outputs
        ]
    }


def _task_template_container_arguments(
    task: Task,
    address: List[str],
) -> List[str]:
    """
    Return a list of arguments to supply to the CLI runtime to run a specific DAG node with a set of inputs and outputs mounted as artifacts.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#outputs
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
                    for input_name in task.inputs
                ],
                *[
                    [
                        "--output",
                        output_name,
                        "{{" + f"outputs.artifacts.{output_name}.path" + "}}",
                    ]
                    for output_name in task.outputs
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
