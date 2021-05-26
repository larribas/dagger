import pytest

import dagger.inputs as inputs
import dagger.outputs as outputs
from dagger.dag import DAG, CyclicDependencyError, DAGOutput, validate_name
from dagger.node import Node
from dagger.serializers import DefaultSerializer

#
# Init
#


def test__init__with_no_nodes():
    with pytest.raises(ValueError) as e:
        DAG(nodes={})

    assert str(e.value) == "A DAG needs to contain at least one node"


def test__init__with_an_invalid_node_name():
    with pytest.raises(ValueError):
        DAG(
            nodes={"invalid_name": Node(lambda: 1)},
        )


def test__init__with_an_invalid_input_name():
    with pytest.raises(ValueError):
        DAG(
            nodes={"my-node": Node(lambda: 1)},
            inputs={"invalid name": inputs.FromParam()},
        )


def test__init__with_invalid_input_type():
    class UnsupportedInput:
        def __init__(self):
            self.serializer = DefaultSerializer

    with pytest.raises(ValueError) as e:
        DAG(
            nodes=dict(
                n=Node(
                    lambda x: x,
                    inputs=dict(x=inputs.FromParam()),
                ),
            ),
            inputs=dict(x=UnsupportedInput()),
        )

    assert (
        str(e.value)
        == "Input 'x' is of type 'UnsupportedInput'. However, DAGs only support the following types of inputs: ['FromParam', 'FromNodeOutput']"
    )


def test__init__with_an_invalid_output_name():
    with pytest.raises(ValueError):
        DAG(
            nodes={
                "my-node": Node(lambda: 1, outputs=dict(x=outputs.FromReturnValue()))
            },
            outputs={"invalid name": DAGOutput("my-node", "x")},
        )


def test__init__with_an_output_that_references_a_nonexistent_node():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={"my-node": Node(lambda: 1)},
            outputs=dict(x=DAGOutput("missing-node", "y")),
        )

    assert (
        str(e.value)
        == "Output 'x' depends on the output of a node named 'missing-node'. However, the DAG does not contain any node with such a name. These are the nodes contained by the DAG: ['my-node']"
    )


def test__init__with_an_output_that_references_a_nonexistent_node_output():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={
                "my-node": Node(lambda: 1, outputs=dict(x=outputs.FromReturnValue()))
            },
            outputs=dict(y=DAGOutput("my-node", "z")),
        )

    assert (
        str(e.value)
        == "Output 'y' depends on the output 'z' of another node named 'my-node'. However, node 'my-node' does not declare any output with such a name. These are the outputs defined by the node: ['x']"
    )


def test__init__with_a_node_that_references_another_that_does_not_exist():
    with pytest.raises(ValueError) as e:
        DAG(
            {
                "my-node": Node(
                    lambda x: 1,
                    inputs=dict(x=inputs.FromNodeOutput("missing-node", "x")),
                ),
            }
        )

    assert (
        str(e.value)
        == "Error validating input 'x' of node 'my-node': This input depends on the output of another node named 'missing-node'. However, the DAG does not define any node with such a name. These are the nodes contained by the DAG: ['my-node']"
    )


def test__init__with_a_node_that_references_an_output_that_does_not_exist():
    with pytest.raises(ValueError) as e:
        DAG(
            {
                "first-node": Node(
                    lambda: 1,
                    outputs=dict(z=outputs.FromReturnValue()),
                ),
                "second-node": Node(
                    lambda x: 1,
                    inputs=dict(x=inputs.FromNodeOutput("first-node", "y")),
                ),
            }
        )

    assert (
        str(e.value)
        == "Error validating input 'x' of node 'second-node': This input depends on the output 'y' of another node named 'first-node'. However, node 'first-node' does not declare any output with such a name. These are the outputs defined by the node: ['z']"
    )


def test__init__with_a_node_that_references_a_dag_input_that_does_not_exist():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={
                "my-node": Node(
                    lambda x: 1,
                    inputs=dict(x=inputs.FromParam()),
                ),
            },
            inputs=dict(z=inputs.FromParam()),
        )

    assert (
        str(e.value)
        == "Error validating input 'x' of node 'my-node': This input depends on a parameter named 'x' being injected into the DAG. However, the DAG does not have any parameter with such a name. These are the parameters the DAG receives: ['z']"
    )


def test__init__with_a_cyclic_dependency():
    with pytest.raises(CyclicDependencyError):
        DAG(
            nodes=dict(
                a=Node(
                    lambda x: x,
                    inputs=dict(x=inputs.FromNodeOutput("a", "x")),
                    outputs=dict(x=outputs.FromReturnValue()),
                ),
            ),
        )


#
# Node execution order
#


def test__node_execution_order__with_no_dependencies():
    dag = DAG(
        nodes=dict(
            one=Node(lambda: 1),
            two=Node(lambda: 2),
        ),
    )
    assert dag.node_execution_order == [{"one", "two"}]


def test__node_execution_order__is_based_on_dependencies():
    dag = DAG(
        nodes=dict(
            second=Node(
                lambda x: x * 2,
                inputs=dict(x=inputs.FromNodeOutput("first", "x")),
                outputs=dict(x=outputs.FromReturnValue()),
            ),
            third=Node(
                lambda x: x * 2,
                inputs=dict(x=inputs.FromNodeOutput("second", "x")),
            ),
            first=Node(
                lambda: 1,
                outputs=dict(x=outputs.FromReturnValue()),
            ),
        ),
    )
    assert dag.node_execution_order == [{"first"}, {"second"}, {"third"}]


#
# validate_name
#


def test__validate_name__with_valid_names():
    valid_names = [
        "param",
        "name-with-dashes",
        "name-with-dashes-and-123",
        "a" * 64,
    ]

    for name in valid_names:
        # We are testing it doesn't raise any validation errors
        validate_name(name)


def test__validate_name__with_invalid_names():
    invalid_names = [
        "",
        "name with spaces",
        "x" * 65,
        "with$ymßols",
        "with_underscores",
    ]

    for name in invalid_names:
        with pytest.raises(ValueError) as e:
            validate_name(name)

        assert (
            str(e.value)
            == f"'{name}' is not a valid name for a DAG. DAG names must comply with the regex ^[a-zA-Z0-9][a-zA-Z0-9-]{{0,63}}$"
        )
