import itertools
import tempfile

import pytest

import dagger.inputs as inputs
import dagger.outputs as outputs
from dagger.dag import DAG, DAGOutput
from dagger.node import Node
from dagger.runtime.cli import invoke


def test__invoke__whole_dag():
    dag = DAG(
        nodes=dict(
            double=Node(
                lambda x: x * 2,
                inputs=dict(x=inputs.FromParam()),
                outputs=dict(x_doubled=outputs.FromReturnValue()),
            ),
            square=Node(
                lambda x: x ** 2,
                inputs=dict(x=inputs.FromNodeOutput("double", "x_doubled")),
                outputs=dict(x_squared=outputs.FromReturnValue()),
            ),
        ),
        inputs=dict(x=inputs.FromParam()),
        outputs=dict(x_doubled_and_squared=DAGOutput("square", "x_squared")),
    )

    with tempfile.NamedTemporaryFile() as x_input:
        x_input.write(b"4")
        x_input.seek(0)

        with tempfile.NamedTemporaryFile() as x_doubled_and_squared_output:
            invoke(
                dag,
                argv=itertools.chain(
                    *[
                        ["--input", "x", x_input.name],
                        [
                            "--output",
                            "x_doubled_and_squared",
                            x_doubled_and_squared_output.name,
                        ],
                    ]
                ),
            )
            assert x_doubled_and_squared_output.read() == b"64"


def test__invoke__selecting_a_node_that_does_not_exist():
    dag = DAG(
        {
            "single-node": Node(lambda: 1),
        }
    )
    with pytest.raises(ValueError) as e:
        invoke(dag, argv=["--node-name", "missing-node"])

    assert (
        str(e.value)
        == "You selected node 'missing-node'. However, this DAG does not contain any node with such a name. These are the names the DAG contains: ['single-node']"
    )


def test__invoke__selecting_a_node():
    dag = DAG(
        nodes=dict(
            square=Node(
                lambda x: x ** 2,
                inputs=dict(x=inputs.FromParam()),
                outputs=dict(x_squared=outputs.FromReturnValue()),
            ),
        ),
        inputs=dict(x=inputs.FromParam()),
        outputs=dict(x_squared=DAGOutput("square", "x_squared")),
    )

    with tempfile.NamedTemporaryFile() as x_input:
        x_input.write(b"4")
        x_input.seek(0)

        with tempfile.NamedTemporaryFile() as x_squared_output:
            invoke(
                dag,
                argv=itertools.chain(
                    *[
                        ["--node-name", "square"],
                        ["--input", "x", x_input.name],
                        ["--output", "x_squared", x_squared_output.name],
                    ]
                ),
            )
            assert x_squared_output.read() == b"16"


def test__invoke__selecting_a_node_from_nested_dag():
    dag = DAG(
        {
            "double": Node(
                lambda x: 2 * x,
                inputs=dict(x=inputs.FromParam()),
                outputs=dict(x=outputs.FromReturnValue()),
            ),
            "nested": DAG(
                {
                    "square": Node(
                        lambda x: x ** 2,
                        inputs=dict(x=inputs.FromParam()),
                        outputs=dict(x=outputs.FromReturnValue()),
                    ),
                },
                inputs=dict(x=inputs.FromParam()),
            ),
        },
        inputs=dict(x=inputs.FromParam()),
        outputs=dict(x=DAGOutput("double", "x")),
    )

    with tempfile.NamedTemporaryFile() as x_input:
        x_input.write(b"4")
        x_input.seek(0)

        with tempfile.NamedTemporaryFile() as x_output:
            invoke(
                dag,
                argv=itertools.chain(
                    *[
                        ["--node-name", "nested.square"],
                        ["--input", "x", x_input.name],
                        ["--output", "x", x_output.name],
                    ]
                ),
            )
            assert x_output.read() == b"16"


def test__invoke__selecting_a_nested_node_that_does_not_exist():
    dag = DAG(
        {
            "nested": DAG({"single-node": Node(lambda: 1)}),
        }
    )
    with pytest.raises(ValueError) as e:
        invoke(dag, argv=["--node-name", "nested.missing-node"])

    assert (
        str(e.value)
        == "You selected node 'nested.missing-node'. However, DAG 'nested' does not contain any node with such a name. These are the names the DAG contains: ['single-node']"
    )


def test__invoke__selecting_a_nested_dag():
    dag = DAG(
        {
            "nested": DAG(
                {
                    "square": Node(
                        lambda x: x ** 2,
                        inputs=dict(x=inputs.FromParam()),
                        outputs=dict(x=outputs.FromReturnValue()),
                    ),
                },
                inputs=dict(x=inputs.FromParam()),
                outputs=dict(x=DAGOutput("square", "x")),
            ),
        },
        inputs=dict(x=inputs.FromParam()),
    )

    with tempfile.NamedTemporaryFile() as x_input:
        x_input.write(b"4")
        x_input.seek(0)

        with tempfile.NamedTemporaryFile() as x_output:
            invoke(
                dag,
                argv=itertools.chain(
                    *[
                        ["--node-name", "nested"],
                        ["--input", "x", x_input.name],
                        ["--output", "x", x_output.name],
                    ]
                ),
            )
            assert x_output.read() == b"16"
