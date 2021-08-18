from dagger.dsl.node_outputs import (
    NodeOutputKeyUsage,
    NodeOutputPropertyUsage,
    NodeOutputReference,
    NodeOutputUsage,
)
from dagger.dsl.serialize import Serialize
from dagger.serializer import AsPickle, DefaultSerializer


def test__node_output_usage__conforms_to_protocol():
    assert isinstance(
        NodeOutputUsage(
            invocation_id="x",
            serialize_annotation=Serialize(),
        ),
        NodeOutputReference,
    )


def test__node_output_property_usage__conforms_to_protocol():
    assert isinstance(
        NodeOutputPropertyUsage(
            invocation_id="x",
            output_name="y",
            property_name="z",
            serializer=DefaultSerializer,
        ),
        NodeOutputReference,
    )


def test__node_output_key_usage__conforms_to_protocol():
    assert isinstance(
        NodeOutputKeyUsage(
            invocation_id="x",
            output_name="y",
            key_name="z",
            serializer=DefaultSerializer,
        ),
        NodeOutputReference,
    )


def test__node_output_usage__returns_an_empty_set_of_references_if_it_is_never_used():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(),
    )
    assert len(output.references) == 0


def test__node_output_usage__returns_itself_if_it_is_explicitly_consumed():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(),
    )
    output.consume()
    assert output.references == {output}


def test__node_output_usage__returns_references_to_sub_keys_when_they_are_used():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(b=AsPickle()),
    )

    # Use sub-keys, sometimes repeatedly
    assert isinstance(output["a"], NodeOutputKeyUsage)
    assert isinstance(output["b"], NodeOutputKeyUsage)
    assert isinstance(output["b"], NodeOutputKeyUsage)

    # The original output stores all the references that were used
    assert output.references == {
        NodeOutputKeyUsage(
            invocation_id="x",
            output_name="key_a",
            key_name="a",
            serializer=DefaultSerializer,
        ),
        NodeOutputKeyUsage(
            invocation_id="x",
            output_name="key_b",
            key_name="b",
            serializer=AsPickle(),
        ),
    }


def test__node_output_usage__returns_references_to_sub_properties_when_they_are_used():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(b=AsPickle()),
    )

    # Use sub-keys, sometimes repeatedly
    assert isinstance(output.a, NodeOutputPropertyUsage)
    assert isinstance(output.b, NodeOutputPropertyUsage)
    assert isinstance(output.b, NodeOutputPropertyUsage)

    # The original output stores all the references that were used
    assert output.references == {
        NodeOutputPropertyUsage(
            invocation_id="x",
            output_name="property_a",
            property_name="a",
            serializer=DefaultSerializer,
        ),
        NodeOutputPropertyUsage(
            invocation_id="x",
            output_name="property_b",
            property_name="b",
            serializer=AsPickle(),
        ),
    }


def test__node_output_usage__captures_serializer_from_annotation():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(AsPickle()),
    )
    assert output.serializer == AsPickle()
