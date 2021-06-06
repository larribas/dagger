import tempfile

import pytest

import dagger.input as input
import dagger.output as output
from dagger.dag import DAG, DAGOutput
from dagger.runtime.cli.dag import invoke_dag
from dagger.task import Task

#
# invoke_dag
#


def test__invoke_dag__with_no_inputs_or_outputs():
    invocations = []
    dag = DAG(
        {
            "single-node": Task(lambda: invocations.append(1)),
        }
    )
    invoke_dag(dag)
    assert invocations == [1]


def test__invoke_dag__with_inputs_and_outputs():
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
        x_input.write(b"3")
        x_input.seek(0)

        with tempfile.NamedTemporaryFile() as x_squared_output:
            invoke_dag(
                dag,
                input_locations=dict(x=x_input.name),
                output_locations=dict(x_squared=x_squared_output.name),
            )
            assert x_squared_output.read() == b"9"


def test__invoke_dag__with_missing_input_parameter():
    dag = DAG(
        nodes=dict(one=Task(lambda: 1)),
        inputs=dict(a=input.FromParam()),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.NamedTemporaryFile() as f:
            f.write(b"1")
            f.seek(0)
            invoke_dag(dag, input_locations=dict(x=f.name))

    assert (
        str(e.value)
        == "This DAG is supposed to receive a pointer to an input named 'a'. However, only the following input pointers were supplied: ['x']"
    )


def test__invoke_dag__with_missing_output_parameter():
    dag = DAG(
        nodes=dict(
            one=Task(
                lambda: 1,
                outputs=dict(x=output.FromReturnValue()),
            )
        ),
        outputs=dict(a=DAGOutput("one", "x")),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.NamedTemporaryFile() as f:
            invoke_dag(dag, output_locations=dict(x=f.name))

    assert (
        str(e.value)
        == "This DAG is supposed to receive a pointer to an output named 'a'. However, only the following output pointers were supplied: ['x']"
    )


def test__invoke_dag__propagates_node_exceptions_extending_the_details():
    dag = DAG(
        nodes={
            "always-1": Task(
                lambda: 1,
                outputs=dict(x=output.FromKey("missing-key")),
            ),
        },
    )
    with pytest.raises(TypeError) as e:
        with tempfile.NamedTemporaryFile() as x_output:
            invoke_dag(dag, output_locations=dict(x=x_output.name))

    assert (
        str(e.value)
        == "Error when invoking task 'always-1'. We encountered the following error while attempting to serialize the results of this task: This output is of type FromKey. This means we expect the return value of the function to be a mapping containing, at least, a key named 'missing-key'"
    )
