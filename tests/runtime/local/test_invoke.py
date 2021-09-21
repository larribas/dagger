import tempfile

from dagger.dag import DAG
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromReturnValue
from dagger.runtime.local.invoke import StoreSerializedOutputsInPath, invoke
from dagger.task import Task


def test__invoke__without_parameters():
    dag = DAG(
        nodes=dict(
            square=Task(
                lambda: 1,
                outputs=dict(x=FromReturnValue()),
            ),
        ),
        outputs=dict(x=FromNodeOutput("square", "x")),
    )
    assert invoke(dag) == {"x": 1}


def test__invoke__with_deserialized_outputs():
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
    assert invoke(dag, params={"x": 3}) == {"x_squared": 9}


def test__invoke__with_outputs_stored_in_path():
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
        outputs = invoke(
            dag,
            params={"x": 3},
            outputs=StoreSerializedOutputsInPath(tmp),
        )

        assert "x_squared" in outputs

        with open(outputs["x_squared"].filename, "rb") as f:
            f.read() == b"9"
