from typing import Iterable

import pytest

from dagger.dsl.node_output_partition_usage import NodeOutputPartitionUsage
from dagger.dsl.node_output_reference import NodeOutputReference
from dagger.dsl.node_output_usage import NodeOutputUsage
from dagger.dsl.serialize import Serialize
from dagger.serializer import DefaultSerializer


def test__node_output_partition_usage__conforms_to_protocol():
    assert isinstance(
        NodeOutputPartitionUsage(
            NodeOutputUsage(
                invocation_id="x",
                serialize_annotation=Serialize(),
            ),
        ),
        NodeOutputReference,
    )


def test__node_output_partition_usage__properties():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(),
    )
    output_partition = NodeOutputPartitionUsage(output)
    assert output_partition.invocation_id == "x"
    assert output_partition.output_name == "return_value"
    assert output_partition.is_partitioned is True
    assert output_partition.serializer == DefaultSerializer
    assert output_partition.wrapped_reference == output


def test__node_output_partition_usage__representation():
    output = NodeOutputUsage(
        invocation_id="x",
        serialize_annotation=Serialize(),
    )
    output_partition = NodeOutputPartitionUsage(output)
    assert (
        repr(output_partition)
        == f"NodeOutputPartitionUsage(wrapped_reference={output})"
    )


def test__node_output_partition_usage__is_iterable_but_cannot_be_iterated_over():
    output = NodeOutputPartitionUsage(
        NodeOutputUsage(
            invocation_id="x",
            serialize_annotation=Serialize(),
        ),
    )

    assert isinstance(output, Iterable)

    with pytest.raises(ValueError) as e:
        list(output)

    assert (
        str(e.value)
        == "When defining DAGs through the DSL, you can iterate over the output of a node, if the output of that node is supposed to be partitioned. However, you may not iterate over one of the partitions of that output."
    )
