import warnings

import pytest

from dagger.dag import DAG
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromKey, FromReturnValue
from dagger.runtime.local import invoke
from dagger.serializer import AsJSON, SerializationError
from dagger.task import Task


def test__invoke__unsupported_node():
    class UnsupportedNode:
        pass

    with pytest.raises(NotImplementedError) as e:
        invoke(UnsupportedNode())

    assert (
        str(e.value)
        == "Whoops, we were not expecting a node of type 'UnsupportedNode'. This type of nodes are not supported by the local runtime at the moment. If you believe this may be a bug, please report it to our GitHub repository."
    )


def test__invoke__dag_with_no_inputs_or_outputs():
    invocations = []
    dag = DAG(
        {
            "single-task": Task(lambda: invocations.append(1)),
        }
    )
    assert invoke(dag) == {}
    assert invocations == [1]


def test__invoke__dag_with_inputs_and_outputs():
    dag = DAG(
        nodes=dict(
            square=Task(
                lambda x: x ** 2,
                inputs=dict(x=FromParam()),
                outputs=dict(x_squared=FromReturnValue()),
            ),
        ),
        inputs=dict(x=FromParam()),
        outputs=dict(x_squared=FromNodeOutput("square", "x_squared")),
    )
    assert invoke(dag, params=dict(x=3)) == dict(x_squared=b"9")


def test__invoke__dag_with_missing_input_parameter():
    dag = DAG(
        nodes=dict(one=Task(lambda: 1)),
        inputs=dict(a=FromParam()),
    )
    with pytest.raises(ValueError) as e:
        invoke(dag, params=dict(y=3))

    assert (
        str(e.value)
        == "The parameters supplied to this DAG were supposed to contain the following parameters: ['a']. However, only the following parameters were actually supplied: ['y']. We are missing: ['a']."
    )


def test__invoke__dag_mapping_dag_parameters_to_node_inputs():
    dag = DAG(
        inputs=dict(a=FromParam()),
        outputs=dict(x=FromNodeOutput("times3", "x")),
        nodes=dict(
            times3=Task(
                lambda b: b * 3,
                inputs=dict(b=FromParam("a")),
                outputs=dict(x=FromReturnValue()),
            )
        ),
    )

    assert invoke(dag, params=dict(a=1)) == {"x": b"3"}


def test__invoke__dag_propagates_task_exceptions_extending_the_details():
    dag = DAG(
        nodes=dict(
            square=Task(
                lambda x: x ** 2,
                inputs=dict(x=FromParam()),
                outputs=dict(x_squared=FromKey("missing-key")),
            ),
        ),
        inputs=dict(x=FromParam()),
    )
    with pytest.raises(TypeError) as e:
        invoke(dag, params=dict(x=3))

    assert (
        str(e.value)
        == "Error when invoking node 'square'. We encountered the following error while attempting to serialize the results of this task: This output is of type FromKey. This means we expect the return value of the function to be a mapping containing, at least, a key named 'missing-key'"
    )


def test__invoke__dag_invokes_nodes_in_the_right_order_based_on_their_dependencies():
    dag = DAG(
        nodes={
            "square-number": Task(
                lambda n: n ** 2,
                inputs=dict(n=FromNodeOutput("generate-number", "n")),
                outputs=dict(n=FromReturnValue()),
            ),
            "divide-number-by-three": Task(
                lambda n: n // 3,
                inputs=dict(n=FromNodeOutput("square-number", "n")),
                outputs=dict(n=FromReturnValue()),
            ),
            "generate-number": Task(
                lambda: 9,
                outputs=dict(n=FromReturnValue()),
            ),
        },
        outputs=dict(n=FromNodeOutput("divide-number-by-three", "n")),
    )
    assert invoke(dag) == dict(n=b"27")


def test__invoke__dag_with_nested_dags():
    dag = DAG(
        {
            "outermost": DAG(
                {
                    "come-up-with-a-number": Task(
                        lambda: 1, outputs=dict(x=FromReturnValue())
                    ),
                    "middle": DAG(
                        {
                            "innermost": Task(
                                lambda x: 2 * x,
                                inputs=dict(x=FromParam()),
                                outputs=dict(y=FromReturnValue()),
                            )
                        },
                        inputs=dict(x=FromNodeOutput("come-up-with-a-number", "x")),
                        outputs=dict(yy=FromNodeOutput("innermost", "y")),
                    ),
                },
                outputs=dict(yyy=FromNodeOutput("middle", "yy")),
            )
        },
        outputs=dict(yyyy=FromNodeOutput("outermost", "yyy")),
    )

    assert invoke(dag) == dict(yyyy=b"2")


#
# Tasks
#


def test__invoke__task_without_inputs_or_outputs():
    invocations = []
    task = Task(lambda: invocations.append(1))
    assert invoke(task) == {}
    assert invocations == [1]


def test__invoke__task_with_single_input_and_output():
    task = Task(
        lambda number: number * 2,
        inputs=dict(number=FromParam()),
        outputs=dict(doubled_number=FromReturnValue()),
    )
    assert invoke(task, params=dict(number=2)) == dict(doubled_number=b"4")


def test__invoke__task_with_multiple_inputs_and_outputs():
    task = Task(
        lambda first_name, last_name: dict(
            message=f"Hello {first_name} {last_name}",
            name_length=len(first_name) + len(last_name),
        ),
        inputs=dict(
            first_name=FromParam(),
            last_name=FromParam(),
        ),
        outputs=dict(
            message=FromKey("message"),
            name_length=FromKey("name_length"),
        ),
    )
    assert invoke(task, params=dict(first_name="John", last_name="Doe",),) == dict(
        message=b'"Hello John Doe"',
        name_length=b"7",
    )


def test__invoke__task_with_missing_input_parameter():
    task = Task(
        lambda a, b: 1,
        inputs=dict(
            a=FromParam(),
            b=FromParam(),
        ),
    )
    with pytest.raises(ValueError) as e:
        invoke(task, params={})

    assert (
        str(e.value)
        == "The following parameters are required by the task but were not supplied: ['a', 'b']"
    )


def test__invoke__task_with_mismatched_outputs():
    task = Task(lambda: 1, outputs=dict(a=FromKey("x")))
    with pytest.raises(TypeError) as e:
        invoke(task, params={})

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: This output is of type FromKey. This means we expect the return value of the function to be a mapping containing, at least, a key named 'x'"
    )


def test__invoke__task_with_missing_outputs():
    task = Task(lambda: dict(a=1), outputs=dict(x=FromKey("x")))
    with pytest.raises(ValueError) as e:
        invoke(task, params={})

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: An output of type FromKey('x') expects the return value of the function to be a mapping containing, at least, a key named 'x'"
    )


def test__invoke__task_with_unserializable_outputs():
    task = Task(lambda: dict(a=lambda: 2), outputs=dict(x=FromKey("a")))
    with pytest.raises(SerializationError) as e:
        invoke(task, params={})

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this task: Object of type function is not JSON serializable"
    )


def test__invoke__task_overriding_the_serializer():
    task = Task(
        lambda: {"a": 2},
        outputs=dict(x=FromReturnValue(serializer=AsJSON(indent=1))),
    )
    assert invoke(task, params={}) == {
        "x": b'{\n "a": 2\n}',
    }


def test__invoke__task_with_superfluous_parameters():
    task = Task(lambda: 1)

    with warnings.catch_warnings(record=True) as w:
        invoke(task, params={"a": 1, "b": 2})
        assert len(w) == 1
        assert (
            str(w[0].message)
            == "The following parameters were supplied to the task, but are not necessary: ['a', 'b']"
        )
