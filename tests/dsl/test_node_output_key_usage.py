from typing import Iterable

from dagger.dsl.node_output_key_usage import NodeOutputKeyUsage
from dagger.dsl.node_output_partition_usage import NodeOutputPartitionUsage
from dagger.dsl.node_output_reference import NodeOutputReference
from dagger.serializer import AsPickle, DefaultSerializer


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


def test__node_output_key_usage__properties():
    output = NodeOutputKeyUsage(
        invocation_id="x",
        output_name="y",
        key_name="z",
        serializer=AsPickle(),
    )
    assert output.invocation_id == "x"
    assert output.output_name == "y"
    assert output.key_name == "z"
    assert output.is_partitioned is False
    assert output.serializer == AsPickle()


def test__node_output_key_usage__is_iterable():
    output = NodeOutputKeyUsage(
        invocation_id="x",
        output_name="y",
        key_name="z",
        serializer=DefaultSerializer,
    )

    assert isinstance(output, Iterable)

    # When we iterate over it
    assert list(output) == [NodeOutputPartitionUsage(output)]
    assert output.is_partitioned is True


def test__node_output_key_usage__representation():
    serializer = AsPickle()
    output = NodeOutputKeyUsage(
        invocation_id="x",
        output_name="y",
        key_name="z",
        serializer=serializer,
        references_node_partition=True,
    )
    assert (
        repr(output)
        == f"NodeOutputKeyUsage(invocation_id=x, output_name=y, key_name=z, serializer={serializer}, is_partitioned=False, references_node_partition=True)"
    )


def test__node_output_key_usage__consume_does_nothing():
    a = NodeOutputKeyUsage(
        invocation_id="x",
        output_name="y",
        key_name="z",
        serializer=DefaultSerializer,
    )
    b = NodeOutputKeyUsage(
        invocation_id="x",
        output_name="y",
        key_name="z",
        serializer=DefaultSerializer,
    )
    b.consume()
    assert a == b
