import pytest

from dagger.dsl.node_output_partition_fan_in import NodeOutputPartitionFanIn
from dagger.dsl.node_output_reference import NodeOutputReference
from dagger.dsl.node_output_usage import NodeOutputUsage


def test__node_output_partition_fan_in__conforms_to_protocol():
    assert isinstance(
        NodeOutputPartitionFanIn(
            NodeOutputUsage(invocation_id="x"),
        ),
        NodeOutputReference,
    )


def test__node_output_partition_fan_in__properties():
    output = NodeOutputUsage(invocation_id="x")
    output_partition = NodeOutputPartitionFanIn(output)
    assert output_partition.invocation_id == "x"
    assert output_partition.output_name == "return_value"
    assert output_partition.is_partitioned is False
    assert output_partition.references_node_partition is False
    assert output_partition.wrapped_reference == output


def test__representation():
    output = NodeOutputUsage(invocation_id="x")
    output_partition = NodeOutputPartitionFanIn(output)
    assert (
        repr(output_partition)
        == f"NodeOutputPartitionFanIn(wrapped_reference={output})"
    )


def test__eq():
    output = NodeOutputUsage(invocation_id="x")
    assert NodeOutputPartitionFanIn(output) == NodeOutputPartitionFanIn(output)


def test__iter():
    output = NodeOutputUsage(invocation_id="x")

    with pytest.raises(NotImplementedError):
        iter(NodeOutputPartitionFanIn(output))
