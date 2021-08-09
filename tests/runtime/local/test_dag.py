import pytest

import dagger.input as input
import dagger.output as output
from dagger.dag import DAG, DAGOutput
from dagger.runtime.local.dag import invoke_dag
from dagger.task import Task


def test__invoke_dag__with_no_inputs_or_outputs():
    invocations = []
    dag = DAG(
        {
            "single-task": Task(lambda: invocations.append(1)),
        }
    )
    assert invoke_dag(dag) == {}
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
    assert invoke_dag(dag, params=dict(x=b"3")) == dict(x_squared=b"9")


def test__invoke_dag__with_missing_input_parameter():
    dag = DAG(
        nodes=dict(one=Task(lambda: 1)),
        inputs=dict(a=input.FromParam()),
    )
    with pytest.raises(ValueError) as e:
        invoke_dag(dag, params=dict(y=b"3"))

    assert (
        str(e.value)
        == "The parameters supplied to this DAG were supposed to contain a parameter named 'a', but only the following parameters were actually supplied: ['y']"
    )


def test__invoke_dag__mapping_dag_parameters_to_node_inputs():
    dag = DAG(
        inputs=dict(a=input.FromParam()),
        outputs=dict(x=DAGOutput("times3", "x")),
        nodes=dict(
            times3=Task(
                lambda b: b * 3,
                inputs=dict(b=input.FromParam("a")),
                outputs=dict(x=output.FromReturnValue()),
            )
        ),
    )

    assert invoke_dag(dag, params=dict(a=b"1")) == {"x": b"3"}


def test__invoke_dag__propagates_task_exceptions_extending_the_details():
    dag = DAG(
        nodes=dict(
            square=Task(
                lambda x: x ** 2,
                inputs=dict(x=input.FromParam()),
                outputs=dict(x_squared=output.FromKey("missing-key")),
            ),
        ),
        inputs=dict(x=input.FromParam()),
    )
    with pytest.raises(TypeError) as e:
        invoke_dag(dag, params=dict(x=b"3"))

    assert (
        str(e.value)
        == "Error when invoking node 'square'. We encountered the following error while attempting to serialize the results of this task: This output is of type FromKey. This means we expect the return value of the function to be a mapping containing, at least, a key named 'missing-key'"
    )


def test__invoke_dag__invoke_dags_nodes_in_the_right_order_based_on_their_dependencies():
    dag = DAG(
        nodes={
            "square-number": Task(
                lambda n: n ** 2,
                inputs=dict(n=input.FromNodeOutput("generate-number", "n")),
                outputs=dict(n=output.FromReturnValue()),
            ),
            "divide-number-by-three": Task(
                lambda n: n // 3,
                inputs=dict(n=input.FromNodeOutput("square-number", "n")),
                outputs=dict(n=output.FromReturnValue()),
            ),
            "generate-number": Task(
                lambda: 9,
                outputs=dict(n=output.FromReturnValue()),
            ),
        },
        outputs=dict(n=DAGOutput("divide-number-by-three", "n")),
    )
    assert invoke_dag(dag) == dict(n=b"27")


def test__invoke_dag__with_nested_dags():
    dag = DAG(
        {
            "outermost": DAG(
                {
                    "come-up-with-a-number": Task(
                        lambda: 1, outputs=dict(x=output.FromReturnValue())
                    ),
                    "middle": DAG(
                        {
                            "innermost": Task(
                                lambda x: 2 * x,
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

    assert invoke_dag(dag) == dict(yyyy=b"2")
