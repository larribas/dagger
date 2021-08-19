"""Generate Workflow specifications."""
import itertools
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Sequence

from dagger.dag import DAG, Node
from dagger.dag import SupportedInputs as SupportedDAGInputs
from dagger.dag import validate_parameters
from dagger.input import FromNodeOutput, FromParam
from dagger.runtime.argo.errors import IncompatibilityError
from dagger.runtime.argo.extra_spec_options import with_extra_spec_options
from dagger.task import SupportedInputs as SupportedTaskInputs
from dagger.task import Task

BASE_DAG_NAME = "dag"
INPUT_PATH = "/tmp/inputs/"
OUTPUT_PATH = "/tmp/outputs/"


@dataclass(frozen=True)
class Workflow:
    """
    Configuration for a Workflow. This class will be supplied to the runtime to help it create a workflow that will work on your environment.

    Parameters
    ----------
    container_image
        The URI to the container image Argo will use for each of the tasks

    container_entrypoint_to_dag_cli
        The container's entrypoint.
        It must run the specified DAG via the CLI runtime.
        Check the examples and documentation to understand better how to containerize your projects and expose your DAGs through the container.

    params
        Parameters to inject to the DAG.
        They must match the inputs the DAG expects.

    extra_spec_options
        WorkflowSpec properties to set (if they are not used by the runtime).

    """

    container_image: str
    container_entrypoint_to_dag_cli: List[str] = field(default_factory=list)
    params: Mapping[str, Any] = field(default_factory=dict)
    extra_spec_options: Mapping[str, Any] = field(default_factory=dict)


def workflow_spec(
    dag: DAG,
    workflow: Workflow,
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a WorkflowSpec for the supplied DAG and metadata.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#workflowspec


    Parameters
    ----------
    dag
        The DAG to generate the spec for

    workflow
        The configuration for this workflow

    Raises
    ------
    ValueError
        If any of the extra_spec_options collides with a property used by the runtime.

    IncompatibilityError
        If the runtime is not compatible with the DAG supplied. This is usually the result of an internal bug.
    """
    validate_parameters(inputs=dag.inputs, params=workflow.params)

    spec = {
        "entrypoint": BASE_DAG_NAME,
        "templates": _templates(
            node=dag,
            container_image=workflow.container_image,
            container_command=workflow.container_entrypoint_to_dag_cli,
            params=workflow.params,
        ),
    }

    if workflow.params:
        spec["arguments"] = _workflow_spec_arguments(workflow.params)

    spec = with_extra_spec_options(
        original=spec,
        extra_options=workflow.extra_spec_options,
        context="the Workflow spec",
    )

    return spec


def _workflow_spec_arguments(params: Mapping[str, Any]) -> Mapping[str, Any]:
    return {
        "parameters": [
            {"name": param_name, "value": params[param_name]} for param_name in params
        ],
    }


def _templates(
    node: Node,
    container_image: str,
    container_command: List[str],
    params: Mapping[str, Any],
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

    params
        The parameters supplied to the DAG.

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
                        params=params,
                        address=address,
                    )
                ],
                *[
                    _templates(
                        node=dag.nodes[node_name],
                        address=address + [node_name],
                        container_image=container_image,
                        container_command=container_command,
                        params=params,
                    )
                    for node_name in dag.nodes
                ],
            )
        )
    else:
        human_readable_node_address = (
            f"Node '{'.'.join(address)}'" if address else "This node"
        )
        raise IncompatibilityError(
            f"Whoops. {human_readable_node_address} is of type '{type(node).__name__}'. While this node type may be supported by the DAG, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
        )


def _dag_template(
    dag: DAG,
    params: Mapping[str, Any],
    address: List[str] = None,
) -> Mapping[str, Any]:
    """
    Return a minimal representation of a Template that uses 'tasks' to orchestrate the supplied DAG.

    Spec: https://github.com/argoproj/argo-workflows/blob/v3.0.4/docs/fields.md#template
    """
    address = address or []

    template: dict = {
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
        is_root_dag = len(address) == 0
        if is_root_dag:
            template["inputs"] = {
                "artifacts": _dag_root_artifacts_from_params(dag.inputs, params),
            }
        else:
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

    template["dag"] = with_extra_spec_options(
        original=template["dag"],
        extra_options=dag.runtime_options.get("argo_dag_template_overrides", {}),
        context=".".join(address) if address else "this DAG",
    )

    return template


def _dag_root_artifacts_from_params(
    inputs: Mapping[str, SupportedDAGInputs],
    params: Mapping[str, Any],
) -> Sequence[Mapping[str, Any]]:
    """
    Return a list of artifacts for the root dag template.

    The workflow accepts parameters as plain values. In order to pass these parameters
    to the CLI runtime we need to convert them into artifacts.

    This conversion should only happen in the root dag template.
    """
    artifacts = []

    for input_name in inputs:
        data = "{{workflow.parameters." + input_name + "}}"

        # Most plain values (booleans, numbers, objects or arrays) will work with the JSON serializer
        # Strings, however, need to be surrounded by double quotation marks.
        if isinstance(params[input_name], str):
            data = f'"{data}"'

        artifacts.append({"name": input_name, "raw": {"data": data}})

    return artifacts


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
        return "{{" + f"inputs.artifacts.{input.name or input_name}" + "}}"
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
            "args": _task_template_container_arguments(task=task, address=address),
        },
    }

    if container_command:
        # We create a copy of the entrypoint to prevent the YAML library
        # from using anchors and aliases and make the resulting YAML more
        # explicit. It also allows any other postprocessor to change some
        # of the entrypoints without affecting the rest
        template["container"]["command"] = container_command[:]

    if task.inputs:
        template["inputs"] = _task_template_inputs(task)

    if task.outputs:
        template["outputs"] = _task_template_outputs(task)
        template["volumes"] = [{"name": "outputs", "emptyDir": {}}]
        template["container"]["volumeMounts"] = [
            {"name": "outputs", "mountPath": OUTPUT_PATH}
        ]

    # Overrides
    template["container"] = with_extra_spec_options(
        original=template["container"],
        extra_options=task.runtime_options.get("argo_container_overrides", {}),
        context=".".join(address),
    )

    return with_extra_spec_options(
        original=template,
        extra_options=task.runtime_options.get("argo_template_overrides", {}),
        context=".".join(address),
    )


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
