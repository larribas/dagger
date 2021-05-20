import re
from typing import Dict, List, NamedTuple, Set, Union

from argo_workflows_sdk.dag.topological_sort import (
    CyclicDependencyError,
    topological_sort,
)
from argo_workflows_sdk.inputs import FromNodeOutput, FromParam
from argo_workflows_sdk.inputs import validate_name as validate_input_name
from argo_workflows_sdk.node import Node
from argo_workflows_sdk.node import SupportedInputs as SupportedNodeInputs
from argo_workflows_sdk.node import validate_name as validate_node_name
from argo_workflows_sdk.outputs import validate_name as validate_output_name

VALID_NAME_REGEX = r"^[a-zA-Z0-9][a-zA-Z0-9-]{0,128}$"
VALID_NAME = re.compile(VALID_NAME_REGEX)


SupportedInputs = Union[
    FromParam,
    FromNodeOutput,
]


class DAGOutput(NamedTuple):
    node: str
    output: str


class DAG:
    def __init__(
        self,
        nodes: Dict[str, Node],
        inputs: Dict[str, SupportedInputs] = None,
        outputs: Dict[str, DAGOutput] = None,
    ):
        inputs = inputs or {}
        outputs = outputs or {}

        validate_nodes_are_not_empty(nodes)

        for node_name in nodes:
            validate_node_name(node_name)

        for input_name in inputs:
            validate_input_name(input_name)

        for output_name in outputs:
            validate_output_name(output_name)

        validate_node_input_dependencies(nodes, inputs)
        validate_outputs(nodes, outputs)

        self.nodes = nodes
        self.inputs = inputs
        self.outputs = outputs
        self.__node_execution_order = topological_sort(
            {
                node_name: node_dependencies(node.inputs)
                for node_name, node in nodes.items()
            }
        )

    @property
    def node_execution_order(self) -> List[Set[str]]:
        return self.__node_execution_order


def validate_parameters(
    inputs: Dict[str, SupportedInputs],
    params: Dict[str, bytes],
):
    for input_name in inputs.keys():
        if input_name not in params:
            raise ValueError(
                f"The parameters supplied to this DAG were supposed to contain a parameter named '{input_name}', but only the following parameters were actually supplied: {list(params.keys())}"
            )


__all__ = [
    DAG,
    DAGOutput,
    SupportedInputs,
    # Exceptions
    CyclicDependencyError,
    # Validation,
    validate_parameters,
]


def node_dependencies(node_inputs: Dict[str, SupportedNodeInputs]) -> Set[str]:
    return {
        input_type.node
        for input_type in node_inputs.values()
        if isinstance(input_type, FromNodeOutput)
    }


def validate_name(name: str):
    if not VALID_NAME.match(name):
        raise ValueError(
            f"'{name}' is not a valid name for a DAG. DAG names must comply with the regex {VALID_NAME_REGEX}"
        )


def validate_outputs(
    dag_nodes: Dict[str, Node],
    dag_outputs: Dict[str, DAGOutput],
):
    for output_name, output in dag_outputs.items():
        if output.node not in dag_nodes:
            raise ValueError(
                f"Output '{output_name}' depends on the output of a node named '{output.node}'. However, the DAG does not contain any node with such a name. These are the nodes contained by the DAG: {list(dag_nodes.keys())}"
            )

        referenced_node_outputs = dag_nodes[output.node].outputs
        if output.output not in referenced_node_outputs:
            raise ValueError(
                f"Output '{output_name}' depends on the output '{output.output}' of another node named '{output.node}'. However, node '{output.node}' does not declare any output with such a name. These are the outputs defined by the node: {list(referenced_node_outputs.keys())}"
            )


def validate_nodes_are_not_empty(nodes: Dict[str, Node]):
    if len(nodes) == 0:
        raise ValueError("A DAG needs to contain at least one node")


def validate_node_input_dependencies(
    dag_nodes: Dict[str, Node],
    dag_inputs: Dict[str, SupportedInputs] = None,
):
    for node_name, node in dag_nodes.items():
        for input_name, input_type in node.inputs.items():
            try:
                if isinstance(input_type, FromParam):
                    validate_input_from_param(input_name, dag_inputs)
                elif isinstance(input_type, FromNodeOutput):
                    validate_input_from_node_output(node_name, input_type, dag_nodes)
                else:
                    raise TypeError(
                        f"Inputs of type '{type(input_type)}' are not supported at the moment"
                    )

            except (TypeError, ValueError) as e:
                raise e.__class__(
                    f"Error validating input '{input_name}' of node '{node_name}': {str(e)}"
                )


def validate_input_from_param(
    input_name: str,
    dag_inputs: Dict[str, SupportedInputs],
):
    if input_name not in dag_inputs:
        raise ValueError(
            f"This input depends on a parameter named '{input_name}' being injected into the DAG. However, the DAG does not have any parameter with such a name. These are the parameters the DAG receives: {list(dag_inputs.keys())}"
        )


def validate_input_from_node_output(
    node_name: str,
    input_type: FromNodeOutput,
    dag_nodes: Dict[str, Node],
):
    if input_type.node not in dag_nodes:
        raise ValueError(
            f"This input depends on the output of another node named '{input_type.node}'. However, the DAG does not define any node with such a name. These are the nodes contained by the DAG: {list(dag_nodes.keys())}"
        )

    referenced_node_outputs = dag_nodes[input_type.node].outputs
    if input_type.output not in referenced_node_outputs:
        raise ValueError(
            f"This input depends on the output '{input_type.output}' of another node named '{input_type.node}'. However, node '{input_type.node}' does not declare any output with such a name. These are the outputs defined by the node: {list(referenced_node_outputs.keys())}"
        )
