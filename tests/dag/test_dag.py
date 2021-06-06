import pytest

import dagger.input as input
import dagger.output as output
from dagger.dag import DAG, CyclicDependencyError, DAGOutput
from dagger.serializer import DefaultSerializer
from dagger.task import Task

#
# Init
#


def test__init__with_no_nodes():
    with pytest.raises(ValueError) as e:
        DAG(nodes={})

    assert str(e.value) == "A DAG needs to contain at least one node"


def test__init__with_an_invalid_node_name():
    invalid_names = [
        "",
        "name with spaces",
        "x" * 65,
        "with$ymßols",
        "with_underscores",
    ]

    for name in invalid_names:
        with pytest.raises(ValueError) as e:
            DAG(
                nodes={name: Task(lambda: 1)},
            )

        assert (
            str(e.value)
            == f"'{name}' is not a valid name for a node. Node names must comply with the regex ^[a-zA-Z0-9][a-zA-Z0-9-]{{0,63}}$"
        )


def test__init__with_a_valid_node_name():
    valid_names = [
        "param",
        "name-with-dashes",
        "name-with-dashes-and-123",
        "x" * 64,
    ]

    for name in valid_names:
        # We are testing it doesn't raise any validation errors
        DAG(
            nodes={name: Task(lambda: 1)},
        )


def test__init__with_an_invalid_input_name():
    with pytest.raises(ValueError):
        DAG(
            nodes={"my-node": Task(lambda: 1)},
            inputs={"invalid name": input.FromParam()},
        )


def test__init__with_invalid_input_type():
    class UnsupportedInput:
        def __init__(self):
            self.serializer = DefaultSerializer

    with pytest.raises(TypeError) as e:
        DAG(
            nodes=dict(
                n=Task(
                    lambda x: x,
                    inputs=dict(x=input.FromParam()),
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
                "my-node": Task(lambda: 1, outputs=dict(x=output.FromReturnValue()))
            },
            outputs={"invalid name": DAGOutput("my-node", "x")},
        )


def test__init__with_an_output_that_references_a_nonexistent_node():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={"my-node": Task(lambda: 1)},
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
                "my-node": Task(lambda: 1, outputs=dict(x=output.FromReturnValue()))
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
                "my-node": Task(
                    lambda x: 1,
                    inputs=dict(x=input.FromNodeOutput("missing-node", "x")),
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
                "first-node": Task(
                    lambda: 1,
                    outputs=dict(z=output.FromReturnValue()),
                ),
                "second-node": Task(
                    lambda x: 1,
                    inputs=dict(x=input.FromNodeOutput("first-node", "y")),
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
                "my-node": Task(
                    lambda x: 1,
                    inputs=dict(x=input.FromParam()),
                ),
            },
            inputs=dict(z=input.FromParam()),
        )

    assert (
        str(e.value)
        == "Error validating input 'x' of node 'my-node': This input depends on a parameter named 'x' being injected into the DAG. However, the DAG does not have any parameter with such a name. These are the parameters the DAG receives: ['z']"
    )


def test__init__with_a_cyclic_dependency():
    with pytest.raises(CyclicDependencyError):
        DAG(
            nodes=dict(
                a=Task(
                    lambda x: x,
                    inputs=dict(x=input.FromNodeOutput("a", "x")),
                    outputs=dict(x=output.FromReturnValue()),
                ),
            ),
        )


def test__init__with_nested_dags():
    DAG(
        {
            "outermost": DAG(
                {
                    "come-up-with-a-number": Task(
                        lambda: 1, outputs=dict(x=output.FromReturnValue())
                    ),
                    "middle": DAG(
                        {
                            "innermost": Task(
                                lambda x: x,
                                inputs=dict(x=input.FromParam()),
                                outputs=dict(y=output.FromReturnValue()),
                            )
                        },
                        inputs=dict(
                            x=input.FromNodeOutput("come-up-with-a-number", "x")
                        ),
                        outputs=dict(yy=DAGOutput("innermost", "y")),
                    ),
                },
                outputs=dict(yyy=DAGOutput("middle", "yy")),
            )
        },
        outputs=dict(yyyy=DAGOutput("outermost", "yyy")),
    )
    # Then, no validation exceptions are raised


#
# Node execution order
#


def test__node_execution_order__with_no_dependencies():
    dag = DAG(
        nodes=dict(
            one=Task(lambda: 1),
            two=Task(lambda: 2),
        ),
    )
    assert dag.node_execution_order == [{"one", "two"}]


def test__node_execution_order__is_based_on_dependencies():
    dag = DAG(
        nodes=dict(
            second=Task(
                lambda x: x * 2,
                inputs=dict(x=input.FromNodeOutput("first", "x")),
                outputs=dict(x=output.FromReturnValue()),
            ),
            third=Task(
                lambda x: x * 2,
                inputs=dict(x=input.FromNodeOutput("second", "x")),
            ),
            first=Task(
                lambda: 1,
                outputs=dict(x=output.FromReturnValue()),
            ),
        ),
    )
    assert dag.node_execution_order == [{"first"}, {"second"}, {"third"}]
