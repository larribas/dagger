import argparse
import itertools
import tempfile

import pytest

import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
from argo_workflows_sdk.dag import DAG, DAGOutput
from argo_workflows_sdk.node import Node
from argo_workflows_sdk.runtime.cli import call_arg_parser, invoke

#
# Invoke
#


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
        == "You used the --node-name parameter to select node 'missing-node'. However, this DAG does not contain any node with such a name. These are the names the DAG contains: ['single-node']"
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


#
# call_arg_parser
#


def test__call_args_parser__with_no_args():
    args = call_arg_parser().parse_args([])
    assert args == argparse.Namespace(
        node_name=None,
        inputs=[],
        outputs=[],
    )


def test__call_args_parser__with_node_name():
    args = call_arg_parser().parse_args(["--node-name", "my-node"])
    assert args == argparse.Namespace(
        node_name="my-node",
        inputs=[],
        outputs=[],
    )


def test__call_args_parser__with_several_inputs_and_outputs():
    args = call_arg_parser().parse_args(
        itertools.chain(
            *[
                ["--input", "x", "location-of-x"],
                ["--input", "y", "location-of-y"],
                ["--output", "a", "location-of-a"],
                ["--output", "b", "location-of-b"],
            ]
        )
    )
    assert args == argparse.Namespace(
        node_name=None,
        inputs=[["x", "location-of-x"], ["y", "location-of-y"]],
        outputs=[["a", "location-of-a"], ["b", "location-of-b"]],
    )
