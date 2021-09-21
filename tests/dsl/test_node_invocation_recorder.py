from contextvars import copy_context

import pytest

from dagger.dsl.context import node_invocations
from dagger.dsl.node_invocation_recorder import NodeInvocationRecorder
from dagger.dsl.node_invocations import NodeInvocation, NodeType
from dagger.dsl.node_output_partition_usage import NodeOutputPartitionUsage
from dagger.dsl.node_output_serializer import NodeOutputSerializer
from dagger.dsl.node_output_usage import NodeOutputUsage
from dagger.dsl.parameter_usage import ParameterUsage
from dagger.serializer import AsPickle


def test__task_invocation__with_too_many_positional_arguments():
    def f():
        pass

    recorder = NodeInvocationRecorder(f, node_type=NodeType.TASK)
    with pytest.raises(TypeError) as e:
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
    with pytest.raises(TypeError) as e:
        recorder(1)

    assert (
        str(e.value)
        == "You have invoked the task 'f' with the following arguments: args=(1,) kwargs={}. However, the signature of the function is '(a: int, b: str)'. The following error was raised as a result of this mismatch: missing a required argument: 'b'"
    )


def test__task_invocation__with_a_sequence_of_mixed_types():
    def f(x):
        pass

    output_from_another_node = NodeOutputUsage(invocation_id="x")

    recorder = NodeInvocationRecorder(f, node_type=NodeType.TASK)
    with pytest.raises(ValueError) as e:
        recorder([1, output_from_another_node])

    assert (
        str(e.value)
        == "Argument 'x' of type 'list' is invalid. Arguments of this type may only contain literal/hardcoded values, or references to the same output from a partitioned node."
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

    assert output == NodeOutputUsage(invocation_id)


def test__task_invocation__records_inputs_from_different_sources():
    def my_func(param, node_output):
        pass

    ctx = copy_context()
    invocation_id = "y"
    param_usage = ParameterUsage(name="param-name")
    node_output_usage = NodeOutputUsage(invocation_id="x")

    recorder = NodeInvocationRecorder(
        func=my_func,
        override_id=invocation_id,
        node_type=NodeType.TASK,
    )

    ctx.run(
        recorder,
        param=param_usage,
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
                "param": ParameterUsage(name="param-name"),
                "node_output": node_output_usage,
            },
            output=NodeOutputUsage(invocation_id),
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


def test__task_invocation__takes_into_account_overridden_serializer():
    def my_func(x: int) -> int:
        return x

    ctx = copy_context()
    recorder = NodeInvocationRecorder(
        func=my_func,
        node_type=NodeType.TASK,
        serializer=NodeOutputSerializer(AsPickle()),
    )
    ctx.run(recorder, x=33)

    assert len(ctx[node_invocations]) == 1

    output = ctx[node_invocations][0].output
    assert output.serializer == AsPickle()


def test__task_invocation__with_several_partitioned_inputs():
    def my_func(x, y):
        return x + y

    ctx = copy_context()
    recorder = NodeInvocationRecorder(func=my_func, node_type=NodeType.TASK)

    with pytest.raises(ValueError) as e:
        ctx.run(
            recorder,
            x=NodeOutputPartitionUsage(NodeOutputUsage(invocation_id="x")),
            y=NodeOutputPartitionUsage(NodeOutputUsage(invocation_id="y")),
        )

    assert (
        str(e.value)
        == "The following inputs to this node are partitioned: ['x', 'y']. However, nodes may only be partitioned by one of their inputs. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
    )


def test__task_invocation__with_a_partitioned_input():
    def my_func(x, y):
        return x + y

    ctx = copy_context()
    recorder = NodeInvocationRecorder(func=my_func, node_type=NodeType.TASK)

    x_output = NodeOutputPartitionUsage(NodeOutputUsage(invocation_id="x"))
    y_output = NodeOutputUsage(invocation_id="y")

    ctx.run(recorder, x=x_output, y=y_output)
    invocation = ctx[node_invocations][0]
    assert invocation.partition_by_input == "x"
    assert invocation.inputs == {
        "x": x_output,
        "y": y_output,
    }


def test__representation():
    def my_func(x, y):
        return x + y

    serialize = NodeOutputSerializer(AsPickle())
    recorder = NodeInvocationRecorder(
        func=my_func,
        node_type=NodeType.TASK,
        override_id="my-id",
        runtime_options={"my": "options"},
        serializer=serialize,
    )

    assert (
        repr(recorder)
        == f"NodeInvocationRecorder(func={my_func}, node_type=task, overridden_id=my-id, serializer={repr(serialize)}, runtime_options={{'my': 'options'}})"
    )
