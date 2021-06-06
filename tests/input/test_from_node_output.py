from dagger.input.from_node_output import FromNodeOutput
from dagger.input.protocol import Input
from dagger.serializer import DefaultSerializer
from tests.input.custom_serializer import CustomSerializer


def test__conforms_to_protocol():
    assert isinstance(FromNodeOutput("node", "output"), Input)


def test__exposes_node_and_output_name():
    node_name = "another-node"
    output_name = "another-nodes-output"
    input = FromNodeOutput(node=node_name, output=output_name)
    assert input.node == node_name
    assert input.output == output_name


def test__with_default_serializer():
    input = FromNodeOutput("node", "output")
    assert input.serializer == DefaultSerializer


def test__with_custom_serializer():
    input = FromNodeOutput("node", "output", serializer=CustomSerializer)
    assert input.serializer == CustomSerializer
