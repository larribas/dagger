import pytest

import dagger.inputs as inputs
import dagger.outputs as outputs
from dagger.dag import DAG, DAGOutput
from dagger.node import Node
from dagger.runtime.local.dag import invoke_dag


def test__invoke_dag__with_no_inputs_or_outputs():
    invocations = []
    dag = DAG(
        {
            "single-node": Node(lambda: invocations.append(1)),
        }
    )
    assert invoke_dag(dag) == {}
    assert invocations == [1]


def test__invoke_dag__with_inputs_and_outputs():
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
    assert invoke_dag(dag, params=dict(x=b"3")) == dict(x_squared=b"9")


def test__invoke_dag__with_missing_input_parameter():
    dag = DAG(
        nodes=dict(one=Node(lambda: 1)),
        inputs=dict(a=inputs.FromParam()),
    )
    with pytest.raises(ValueError) as e:
        invoke_dag(dag, params=dict(y=b"3"))

    assert (
        str(e.value)
        == "The parameters supplied to this DAG were supposed to contain a parameter named 'a', but only the following parameters were actually supplied: ['y']"
    )


def test__invoke_dag__propagates_node_exceptions_extending_the_details():
    dag = DAG(
        nodes=dict(
            square=Node(
                lambda x: x ** 2,
                inputs=dict(x=inputs.FromParam()),
                outputs=dict(x_squared=outputs.FromKey("missing-key")),
            ),
        ),
        inputs=dict(x=inputs.FromParam()),
    )
    with pytest.raises(TypeError) as e:
        invoke_dag(dag, params=dict(x=b"3"))

    assert (
        str(e.value)
        == "Error when invoking node 'square'. We encountered the following error while attempting to serialize the results of this node: This output is of type FromKey. This means we expect the return value of the function to be a dictionary containing, at least, a key named 'missing-key'"
    )


def test__invoke_dag__invoke_dags_nodes_in_the_right_order_based_on_their_dependencies():
    dag = DAG(
        nodes={
            "square-number": Node(
                lambda n: n ** 2,
                inputs=dict(n=inputs.FromNodeOutput("generate-number", "n")),
                outputs=dict(n=outputs.FromReturnValue()),
            ),
            "divide-number-by-three": Node(
                lambda n: n // 3,
                inputs=dict(n=inputs.FromNodeOutput("square-number", "n")),
                outputs=dict(n=outputs.FromReturnValue()),
            ),
            "generate-number": Node(
                lambda: 9,
                outputs=dict(n=outputs.FromReturnValue()),
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
                    "come-up-with-a-number": Node(
                        lambda: 1, outputs=dict(x=outputs.FromReturnValue())
                    ),
                    "middle": DAG(
                        {
                            "innermost": Node(
                                lambda x: 2 * x,
                                inputs=dict(x=inputs.FromParam()),
                                outputs=dict(y=outputs.FromReturnValue()),
                            )
                        },
                        inputs=dict(
                            x=inputs.FromNodeOutput("come-up-with-a-number", "x")
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
