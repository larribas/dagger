import dagger.dsl.dsl as dsl
from dagger.dsl.node_invocation_recorder import NodeInvocationRecorder
from dagger.dsl.node_invocations import NodeType


def test__task():
    def f():
        pass

    assert dsl.task(f) == NodeInvocationRecorder(
        f,
        node_type=NodeType.TASK,
    )


def test__dag():
    def g():
        pass

    assert dsl.DAG(g) == NodeInvocationRecorder(
        g,
        node_type=NodeType.DAG,
    )
