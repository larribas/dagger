from dagger.input.from_node_output import FromNodeOutput
from dagger.input.protocol import Input
from dagger.serializer import DefaultSerializer
from tests.input.custom_serializer import CustomSerializer


def test__conforms_to_protocol():
    assert isinstance(FromNodeOutput("node", "output"), Input)


def test__exposes_node_and_output_name():
    node_name = "another-node"
    output_name = "another-nodes-output"
    input_ = FromNodeOutput(node=node_name, output=output_name)
    assert input_.node == node_name
    assert input_.output == output_name


def test__with_default_serializer():
    input_ = FromNodeOutput("node", "output")
    assert input_.serializer == DefaultSerializer


def test__with_custom_serializer():
    serializer = CustomSerializer()
    input_ = FromNodeOutput("node", "output", serializer=serializer)
    assert input_.serializer == serializer


def test__representation():
    serializer = CustomSerializer()
    input_ = FromNodeOutput("my-node", "my-output", serializer=serializer)
    assert (
        repr(input_)
        == f"FromNodeOutput(node=my-node, output=my-output, serializer={repr(serializer)})"
    )
