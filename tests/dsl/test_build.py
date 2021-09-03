from dagger.dsl.build import (
    _build_dag_outputs,
    _build_node,
    _build_node_input,
    _build_task_output,
    _translate_invocation_ids_into_readable_names,
)
from dagger.dsl.node_invocations import NodeInvocation, NodeType
from dagger.dsl.node_output_key_usage import NodeOutputKeyUsage
from dagger.dsl.node_output_property_usage import NodeOutputPropertyUsage
from dagger.dsl.node_output_usage import NodeOutputUsage
from dagger.dsl.parameter_usage import ParameterUsage
from dagger.dsl.serialize import Serialize
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromKey, FromProperty, FromReturnValue
from dagger.serializer import AsJSON, AsPickle
from dagger.task import Task


def test__translate_invocation_ids_into_readable_names():
    def invocation(id: str, name: str) -> NodeInvocation:
        return NodeInvocation(
            id=id,
            name=name,
            node_type=NodeType.TASK,
            func=lambda: 1,
            inputs={},
            output=NodeOutputUsage(
                id,
                serialize_annotation=Serialize(),
            ),
        )

    node_names_by_id = _translate_invocation_ids_into_readable_names(
        [
            invocation("1", "x"),
            invocation("2", "y"),
            invocation("3", "y"),
            invocation("4", "y"),
            invocation("5", "z"),
        ]
    )

    assert node_names_by_id == {
        "1": "x",
        "2": "y-1",
        "3": "y-2",
        "4": "y-3",
        "5": "z",
    }


def test__build_node_input__param_usage():
    assert _build_node_input(ParameterUsage(), node_names_by_id={}) == FromParam()
    assert _build_node_input(
        ParameterUsage(name="x"), node_names_by_id={}
    ) == FromParam("x")


def test__build_node_input__node_output_reference():
    assert _build_node_input(
        NodeOutputUsage(
            invocation_id="id",
            serialize_annotation=Serialize(AsPickle()),
        ),
        node_names_by_id={"id": "name"},
    ) == FromNodeOutput(
        "name",
        "return_value",
        serializer=AsPickle(),
    )


def test__build_task_output__return_value():
    assert _build_task_output(
        NodeOutputUsage(
            invocation_id="id",
            serialize_annotation=Serialize(AsPickle()),
        )
    ) == FromReturnValue(
        serializer=AsPickle(),
    )


def test__build_task_output__key():
    assert _build_task_output(
        NodeOutputKeyUsage(
            invocation_id="id",
            output_name="output",
            key_name="key",
            serializer=AsPickle(),
        )
    ) == FromKey(
        "key",
        serializer=AsPickle(),
    )


def test__build_task_output__property():
    assert _build_task_output(
        NodeOutputPropertyUsage(
            invocation_id="id",
            output_name="output",
            property_name="prop",
            serializer=AsPickle(),
        )
    ) == FromProperty(
        "prop",
        serializer=AsPickle(),
    )


def test__build_node__task():
    def f(param, node_output):
        pass

    this_task_id = "uid-2"
    this_task_output_usage = NodeOutputUsage(
        this_task_id,
        serialize_annotation=Serialize(),
    )
    this_task_output_usage.consume()
    this_task_output_usage["k"]
    this_task_output_usage.prop

    another_node_id = "uid-1"
    another_node_output_usage = NodeOutputUsage(
        another_node_id,
        serialize_annotation=Serialize(AsPickle()),
    )
    another_node_output_usage.consume()

    built_task = _build_node(
        NodeInvocation(
            id=this_task_id,
            name="f",
            node_type=NodeType.TASK,
            func=f,
            inputs={
                "param": ParameterUsage(),
                "node_output": another_node_output_usage,
            },
            output=this_task_output_usage,
        ),
        node_names_by_id={
            another_node_id: "another-node",
            this_task_id: "this-task",
        },
    )

    assert built_task == Task(
        f,
        inputs={
            "param": FromParam(),
            "node_output": FromNodeOutput(
                "another-node",
                "return_value",
                serializer=AsPickle(),
            ),
        },
        outputs={
            "return_value": FromReturnValue(),
            "key_k": FromKey("k"),
            "property_prop": FromProperty("prop"),
        },
    )


def test__build_dag_outputs__when_none_are_returned():
    assert (
        _build_dag_outputs(
            None,
            node_names_by_id={},
        )
        == {}
    )


def test__build_dag_outputs__when_a_single_output_is_returned():
    assert _build_dag_outputs(
        NodeOutputKeyUsage(
            invocation_id="id",
            output_name="output",
            key_name="key",
            serializer=AsPickle(),
        ),
        node_names_by_id={"id": "name"},
    ) == {
        "return_value": FromNodeOutput(
            "name",
            "output",
            serializer=AsPickle(),
        )
    }


def test__build_dag_outputs__when_multiple_outputs_are_returned():
    assert _build_dag_outputs(
        {
            "x": NodeOutputKeyUsage(
                invocation_id="id-1",
                output_name="output",
                key_name="key",
                serializer=AsPickle(),
            ),
            "y": NodeOutputUsage(
                invocation_id="id-2",
                serialize_annotation=Serialize(AsJSON(indent=1)),
            ),
        },
        node_names_by_id={
            "id-1": "name-1",
            "id-2": "name-2",
        },
    ) == {
        "x": FromNodeOutput(
            "name-1",
            "output",
            serializer=AsPickle(),
        ),
        "y": FromNodeOutput(
            "name-2",
            "return_value",
            serializer=AsJSON(indent=1),
        ),
    }


def test__build_dag_outputs__consumes_node_output_usages():
    node_output_usage = NodeOutputUsage(
        invocation_id="id",
        serialize_annotation=Serialize(),
    )
    _build_dag_outputs(
        {"x": node_output_usage},
        node_names_by_id={"id": "name"},
    )
    assert node_output_usage.references == {node_output_usage}
