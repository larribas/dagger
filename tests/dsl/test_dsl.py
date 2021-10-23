import dagger.dsl.dsl as dsl
from dagger.dsl import build
from dagger.dsl.node_invocation_recorder import NodeInvocationRecorder
from dagger.dsl.node_invocations import NodeType
from dagger.dsl.node_output_serializer import NodeOutputSerializer
from dagger.runtime.cli import invoke
from dagger.serializer import AsPickle


def test__task__as_decorator():
    @dsl.task()
    def f():
        pass

    assert f == NodeInvocationRecorder(
        f.func,
        node_type=NodeType.TASK,
    )


def test__task__as_decorator_with_overrides():
    serializer = NodeOutputSerializer(AsPickle())
    runtime_options = {"my": "options"}

    @dsl.task(serializer=serializer, runtime_options=runtime_options)
    def f():
        pass

    assert f == NodeInvocationRecorder(
        f.func,
        node_type=NodeType.TASK,
        serializer=serializer,
        runtime_options=runtime_options,
    )


def test__dag__as_decorator():
    @dsl.DAG()
    def g():
        pass

    assert g == NodeInvocationRecorder(
        g.func,
        node_type=NodeType.DAG,
    )


def test__dag__as_decorator_with_overrides():
    runtime_options = {"my": "options"}

    @dsl.DAG(runtime_options=runtime_options)
    def g():
        pass

    assert g == NodeInvocationRecorder(
        g.func,
        node_type=NodeType.DAG,
        runtime_options=runtime_options,
    )


def test__dag__with_default_value():
    @dsl.task()
    def f(a, b=2):
        return a + b

    @dsl.DAG()
    def d(x=3):
        return f(x)

    assert invoke(build(d)) == 5
