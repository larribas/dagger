from typing import Iterable

import pytest

from dagger.dsl.node_output_key_usage import NodeOutputKeyUsage
from dagger.dsl.node_output_partition_usage import NodeOutputPartitionUsage
from dagger.dsl.node_output_property_usage import NodeOutputPropertyUsage
from dagger.dsl.node_output_reference import NodeOutputReference
from dagger.dsl.node_output_usage import NodeOutputUsage
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


def test__node_output_usage__properties():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(),
    )
    assert output.invocation_id == "x"
    assert output.output_name == "return_value"
    assert output.serializer == DefaultSerializer
    assert output.is_partitioned is False
    assert output.references == set()


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


def test__node_output_usage__accessing_meta_attribute():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(),
    )
    with pytest.raises(ValueError) as e:
        output.__x__

    assert (
        str(e.value)
        == "You are trying to reference attribute named '__x__'. Attributes of the form __name__ are used internally by Python and they are not available within the scope of the dagger DSL."
    )


def test__node_output_usage__is_iterable():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(),
    )

    assert isinstance(output, Iterable)

    # When we iterate over it
    assert list(output) == [NodeOutputPartitionUsage(output)]
    assert output.is_partitioned is True
    assert output.references == {output}
