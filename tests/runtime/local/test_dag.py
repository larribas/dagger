import tempfile

import pytest

from dagger.dag import DAG
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromKey, FromReturnValue
from dagger.runtime.local.dag import invoke_dag
from dagger.runtime.local.output import deserialized_outputs
from dagger.task import Task


def test__invoke_dag__with_no_inputs_or_outputs():
    invocations = []
    dag = DAG(
        {
            "single-task": Task(lambda: invocations.append(1)),
        }
    )
    with tempfile.TemporaryDirectory() as tmp:
        assert invoke_dag(dag, params={}, output_path=tmp) == {}

    assert invocations == [1]


def test__invoke_dag__with_inputs_and_outputs():
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
    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_dag(
            dag,
            params={"x": 3},
            output_path=tmp,
        )
        assert deserialized_outputs(outputs) == {"x_squared": 9}


def test__invoke_dag__with_missing_input_parameter():
    dag = DAG(
        nodes=dict(one=Task(lambda: 1)),
        inputs=dict(a=FromParam()),
    )
    with pytest.raises(ValueError) as e:
        with tempfile.TemporaryDirectory() as tmp:
            invoke_dag(dag, params=dict(y=3), output_path=tmp)

    assert (
        str(e.value)
        == "The parameters supplied to this DAG were supposed to contain the following parameters: ['a']. However, only the following parameters were actually supplied: ['y']. We are missing: ['a']."
    )


def test__invoke_dag__mapping_dag_parameters_to_node_inputs():
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

    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_dag(dag, params=dict(a=1), output_path=tmp)
        assert deserialized_outputs(outputs) == {"x": 3}


def test__invoke_dag__propagates_task_exceptions_extending_the_details():
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
        with tempfile.TemporaryDirectory() as tmp:
            invoke_dag(dag, params=dict(x=3), output_path=tmp)

    assert (
        str(e.value)
        == "Error when invoking node 'square'. We encountered the following error while attempting to serialize the results of this task: This output is of type FromKey. This means we expect the return value of the function to be a mapping containing, at least, a key named 'missing-key'"
    )


def test__invoke_dag__invokes_nodes_in_the_right_order_based_on_their_dependencies():
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
    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_dag(dag, params={}, output_path=tmp)
        assert deserialized_outputs(outputs) == {"n": 27}


def test__invoke_dag__with_nested_dags():
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

    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_dag(dag, params={}, output_path=tmp)
        assert deserialized_outputs(outputs) == {"yyyy": 2}


def test__invoke_dag__with_dynamic_partitions():
    dag = DAG(
        {
            "generate-partitions": Task(
                lambda: ["a", "b", "c"],
                outputs=dict(letters=FromReturnValue(is_partitioned=True)),
            ),
            "run-for-each-partition": Task(
                lambda letter: f"letter '{letter}'",
                inputs=dict(letter=FromNodeOutput("generate-partitions", "letters")),
                outputs=dict(message=FromReturnValue()),
                partition_by_input="letter",
            ),
            "gather-messages": Task(
                lambda messages: f"Got messages: {', and '.join(messages)}.",
                inputs=dict(
                    messages=FromNodeOutput("run-for-each-partition", "message")
                ),
                outputs=dict(result=FromReturnValue()),
            ),
        },
        outputs=dict(result=FromNodeOutput("gather-messages", "result")),
    )

    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_dag(dag, params={}, output_path=tmp)
        assert deserialized_outputs(outputs) == {
            "result": "Got messages: letter 'a', and letter 'b', and letter 'c'."
        }


def test__invoke_dag__with_partitioned_output():
    dag = DAG(
        outputs=dict(result=FromNodeOutput("fan-out", "numbers")),
        nodes={
            "fan-out": Task(
                lambda: [1, 2, 3],
                outputs={
                    "numbers": FromReturnValue(is_partitioned=True),
                },
            ),
        },
    )

    with tempfile.TemporaryDirectory() as tmp:
        outputs = invoke_dag(dag, params={}, output_path=tmp)
        assert deserialized_outputs(outputs) == {
            "result": [1, 2, 3],
        }


def test__invoke_dag__with_partitions_but_invalid_outputs():
    dag = DAG(
        nodes={
            "generate-single-number": Task(
                lambda: 1,
                outputs={"n": FromReturnValue(is_partitioned=True)},
            ),
            "poorly-partitioned-task": Task(
                lambda x: x,
                inputs=dict(x=FromNodeOutput("generate-single-number", "n")),
                partition_by_input="x",
            ),
        },
    )

    with pytest.raises(TypeError) as e:
        with tempfile.TemporaryDirectory() as tmp:
            invoke_dag(dag, params={}, output_path=tmp)

    assert (
        str(e.value)
        == "Error when invoking node 'generate-single-number'. We encountered the following error while attempting to serialize the results of this task: Output 'n' was declared as a partitioned output, but the return value was not an iterable (instead, it was of type 'int'). Partitioned outputs should be iterables of values (e.g. lists or sets). Each value in the iterable must be serializable with the serializer defined in the output."
    )
