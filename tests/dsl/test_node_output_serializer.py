from dagger.dsl.node_output_serializer import NodeOutputSerializer
from dagger.serializer import AsJSON, AsPickle, DefaultSerializer

#
# Serialize
#


def test__serialize__default():
    serializer = NodeOutputSerializer()
    assert serializer.root == DefaultSerializer
    assert serializer.sub_output("missing") is None


def test__serialize__single_output():
    serializer = NodeOutputSerializer(AsPickle())
    assert serializer.root == AsPickle()
    assert serializer.sub_output("missing") is None


def test__serialize__root_output_and_others():
    serializer = NodeOutputSerializer(
        root=AsPickle(), a=AsJSON(indent=1), b=AsJSON(indent=3)
    )
    assert serializer.root == AsPickle()
    assert serializer.sub_output("a") == AsJSON(indent=1)
    assert serializer.sub_output("b") == AsJSON(indent=3)
    assert serializer.sub_output("missing") is None


def test__serialize__multiple_sub_outputs_omitting_root():
    serializer = NodeOutputSerializer(a=AsJSON(indent=1), b=AsJSON(indent=3))
    assert serializer.root == DefaultSerializer
    assert serializer.sub_output("a") == AsJSON(indent=1)
    assert serializer.sub_output("b") == AsJSON(indent=3)
    assert serializer.sub_output("missing") is None


def test__serialize__representation():
    root_serializer = AsPickle()
    a_serializer = AsJSON(indent=1)
    b_serializer = AsJSON(indent=3)
    serializer = NodeOutputSerializer(
        root=root_serializer,
        a=a_serializer,
        b=b_serializer,
    )
    assert (
        repr(serializer)
        == f"Serialize(root={root_serializer}, a={a_serializer}, b={b_serializer})"
    )
