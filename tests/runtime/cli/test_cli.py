import itertools
import tempfile

import pytest

import dagger.input as input
import dagger.output as output
from dagger.dag import DAG, DAGOutput
from dagger.runtime.cli import invoke
from dagger.task import Task


def test__invoke__whole_dag():
    dag = DAG(
        nodes=dict(
            double=Task(
                lambda x: x * 2,
                inputs=dict(x=input.FromParam()),
                outputs=dict(x_doubled=output.FromReturnValue()),
            ),
            square=Task(
                lambda x: x ** 2,
                inputs=dict(x=input.FromNodeOutput("double", "x_doubled")),
                outputs=dict(x_squared=output.FromReturnValue()),
            ),
        ),
        inputs=dict(x=input.FromParam()),
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
            "single-node": Task(lambda: 1),
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
            square=Task(
                lambda x: x ** 2,
                inputs=dict(x=input.FromParam()),
                outputs=dict(x_squared=output.FromReturnValue()),
            ),
        ),
        inputs=dict(x=input.FromParam()),
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
            "double": Task(
                lambda x: 2 * x,
                inputs=dict(x=input.FromParam()),
                outputs=dict(x=output.FromReturnValue()),
            ),
            "nested": DAG(
                {
                    "square": Task(
                        lambda x: x ** 2,
                        inputs=dict(x=input.FromParam()),
                        outputs=dict(x=output.FromReturnValue()),
                    ),
                },
                inputs=dict(x=input.FromParam()),
            ),
        },
        inputs=dict(x=input.FromParam()),
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
            "nested": DAG({"single-node": Task(lambda: 1)}),
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
                    "square": Task(
                        lambda x: x ** 2,
                        inputs=dict(x=input.FromParam()),
                        outputs=dict(x=output.FromReturnValue()),
                    ),
                },
                inputs=dict(x=input.FromParam()),
                outputs=dict(x=DAGOutput("square", "x")),
            ),
        },
        inputs=dict(x=input.FromParam()),
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
