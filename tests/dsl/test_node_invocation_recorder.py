from contextvars import copy_context
from typing import Annotated

import pytest

from dagger.dsl.context import node_invocations
from dagger.dsl.errors import NodeInvokedWithMismatchedArgumentsError
from dagger.dsl.node_invocation_recorder import NodeInvocationRecorder
from dagger.dsl.node_invocations import NodeInvocation, NodeType
from dagger.dsl.node_outputs import NodeOutputUsage
from dagger.dsl.parameter_usage import ParameterUsage
from dagger.dsl.serialize import Serialize
from dagger.serializer import AsPickle


def test__task_invocation__with_too_many_positional_arguments():
    def f():
        pass

    recorder = NodeInvocationRecorder(f, node_type=NodeType.TASK)
    with pytest.raises(NodeInvokedWithMismatchedArgumentsError) as e:
        ctx = copy_context()
        ctx.run(recorder, 1, a=2)

    assert (
        str(e.value)
        == "You have invoked the task 'f' with the following arguments: args=(1,) kwargs={'a': 2}. However, the signature of the function is '()'. The following error was raised as a result of this mismatch: too many positional arguments"
    )


def test__task_invocation__with_missing_argument():
    def f(a: int, b: str):
        pass

    recorder = NodeInvocationRecorder(f, node_type=NodeType.TASK)
    with pytest.raises(NodeInvokedWithMismatchedArgumentsError) as e:
        recorder(1)

    assert (
        str(e.value)
        == "You have invoked the task 'f' with the following arguments: args=(1,) kwargs={}. However, the signature of the function is '(a: int, b: str)'. The following error was raised as a result of this mismatch: missing a required argument: 'b'"
    )


def test__task_invocation__returns_node_output_usage():
    def my_func():
        pass

    ctx = copy_context()
    invocation_id = "y"

    recorder = NodeInvocationRecorder(
        func=my_func,
        override_id=invocation_id,
        node_type=NodeType.TASK,
    )
    output = ctx.run(recorder)

    assert output == NodeOutputUsage(
        invocation_id,
        serialize_annotation=Serialize(),
    )


def test__task_invocation__records_inputs_from_different_sources():
    def my_func(param, param_with_name, node_output):
        pass

    ctx = copy_context()
    invocation_id = "y"
    param_usage = ParameterUsage()
    param_with_name_usage = ParameterUsage(name="overridden")
    node_output_usage = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(),
    )

    recorder = NodeInvocationRecorder(
        func=my_func,
        override_id=invocation_id,
        node_type=NodeType.TASK,
    )

    ctx.run(
        recorder,
        param=param_usage,
        param_with_name=param_with_name_usage,
        node_output=node_output_usage,
    )

    assert node_output_usage.references == {node_output_usage}
    assert ctx[node_invocations] == [
        NodeInvocation(
            id=invocation_id,
            name="my-func",
            node_type=NodeType.TASK,
            func=my_func,
            inputs={
                "param": ParameterUsage(),
                "param_with_name": ParameterUsage(name=param_with_name_usage.name),
                "node_output": node_output_usage,
            },
            output=NodeOutputUsage(invocation_id, serialize_annotation=Serialize()),
            runtime_options={},
        ),
    ]


def test__task_invocation__generates_unique_invocation_ids():
    def my_func():
        pass

    ctx = copy_context()
    n_invocations = 3
    recorder = NodeInvocationRecorder(my_func, node_type=NodeType.TASK)

    for _ in range(n_invocations):
        ctx.run(recorder)

    assert len(ctx[node_invocations]) == n_invocations
    assert len({inv.id for inv in ctx[node_invocations]}) == n_invocations


def test__task_invocation__takes_into_account_serialize_annotations():
    def my_func(x: int) -> Annotated[int, Serialize(AsPickle())]:
        return x

    ctx = copy_context()
    recorder = NodeInvocationRecorder(func=my_func, node_type=NodeType.TASK)
    ctx.run(recorder, x=33)

    assert len(ctx[node_invocations]) == 1

    output = ctx[node_invocations][0].output
    assert output.serializer == AsPickle()
