from contextvars import copy_context

from dagger.dsl.context import node_invocations
from dagger.dsl.node_invocations import NodeInvocation, NodeType
from dagger.dsl.node_outputs import NodeOutputUsage
from dagger.dsl.serialize import Serialize


def test__default_context__is_propagated_only_when_copying_the_current_context():
    ctx = copy_context()
    ctx.run(append_invocation, name="x")
    assert [i.name for i in ctx[node_invocations]] == ["x"]

    sibling_ctx = copy_context()
    sibling_ctx.run(append_invocation, name="y")
    assert [i.name for i in sibling_ctx[node_invocations]] == ["y"]

    child_ctx = ctx.run(copy_context)
    child_ctx.run(append_invocation, name="y")
    assert [i.name for i in child_ctx[node_invocations]] == ["x", "y"]


def test__node_invocation_context__survives_multiple_invocations():
    ctx = copy_context()
    ctx.run(append_invocation, name="x")
    ctx.run(append_invocation, name="y")

    invocations = ctx[node_invocations]

    assert len(invocations) == 2
    assert invocations[0].name == "x"
    assert invocations[1].name == "y"


def test__node_invocation_context__isolates_multiple_invocations():
    ctx = copy_context()
    ctx.run(append_invocation, name="x")
    assert [i.name for i in ctx[node_invocations]] == ["x"]

    ctx = copy_context()
    ctx.run(append_invocation, name="y")
    assert [i.name for i in ctx[node_invocations]] == ["y"]


def append_invocation(name: str):
    new_invocation = NodeInvocation(
        id=name,
        name=name,
        node_type=NodeType.DAG,
        func=lambda: 1,
        inputs={},
        output=NodeOutputUsage("x", serialize_annotation=Serialize()),
    )

    invocations = node_invocations.get([])
    invocations.append(new_invocation)
    node_invocations.set(invocations)
