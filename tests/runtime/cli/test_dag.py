import tempfile

import pytest

import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
from argo_workflows_sdk.dag import DAG, DAGOutput
from argo_workflows_sdk.node import Node
from argo_workflows_sdk.runtime.cli.dag import invoke

#
# Invoke
#


def test__invoke__with_no_inputs_or_outputs():
    invocations = []
    dag = DAG(
        {
            "single-node": Node(lambda: invocations.append(1)),
        }
    )
    invoke(dag)
    assert invocations == [1]


def test__invoke__with_inputs_and_outputs():
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
        x_input.write(b"3")
        x_input.seek(0)

        with tempfile.NamedTemporaryFile() as x_squared_output:
            invoke(
                dag,
                input_locations=dict(x=x_input.name),
                output_locations=dict(x_squared=x_squared_output.name),
            )
            assert x_squared_output.read() == b"9"


def test__invoke__with_missing_input_parameter():
    dag = DAG(
        nodes=dict(one=Node(lambda: 1)),
        inputs=dict(a=inputs.FromParam()),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.NamedTemporaryFile() as f:
            f.write(b"1")
            f.seek(0)
            invoke(dag, input_locations=dict(x=f.name))

    assert (
        str(e.value)
        == "This DAG is supposed to receive a pointer to an input named 'a'. However, only the following input pointers were supplied: ['x']"
    )


def test__invoke__with_missing_output_parameter():
    dag = DAG(
        nodes=dict(
            one=Node(
                lambda: 1,
                outputs=dict(x=outputs.FromReturnValue()),
            )
        ),
        outputs=dict(a=DAGOutput("one", "x")),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.NamedTemporaryFile() as f:
            invoke(dag, output_locations=dict(x=f.name))

    assert (
        str(e.value)
        == "This DAG is supposed to receive a pointer to an output named 'a'. However, only the following output pointers were supplied: ['x']"
    )


def test__invoke__propagates_node_exceptions_extending_the_details():
    dag = DAG(
        nodes={
            "always-1": Node(
                lambda: 1,
                outputs=dict(x=outputs.FromKey("missing-key")),
            ),
        },
    )
    with pytest.raises(TypeError) as e:
        with tempfile.NamedTemporaryFile() as x_output:
            invoke(dag, output_locations=dict(x=x_output.name))

    assert (
        str(e.value)
        == "Error when invoking node 'always-1'. We encountered the following error while attempting to serialize the results of this node: This output is of type FromKey. This means we expect the return value of the function to be a dictionary containing, at least, a key named 'missing-key'"
    )
