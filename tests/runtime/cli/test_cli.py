import itertools
import json
import os
import tempfile

import pytest

from dagger.dag import DAG
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromReturnValue
from dagger.runtime.cli.cli import invoke
from dagger.runtime.cli.locations import (
    PARTITION_MANIFEST_FILENAME,
    store_output_in_location,
)
from dagger.runtime.local import PartitionedOutput
from dagger.serializer import AsPickle
from dagger.task import Task


def test__invoke__whole_dag():
    dag = DAG(
        nodes=dict(
            double=Task(
                lambda x: x * 2,
                inputs=dict(x=FromParam()),
                outputs=dict(x_doubled=FromReturnValue()),
            ),
            square=Task(
                lambda x: x ** 2,
                inputs=dict(x=FromNodeOutput("double", "x_doubled")),
                outputs=dict(x_squared=FromReturnValue()),
            ),
        ),
        inputs=dict(x=FromParam()),
        outputs=dict(x_doubled_and_squared=FromNodeOutput("square", "x_squared")),
    )

    with tempfile.TemporaryDirectory() as tmp:
        x_input = os.path.join(tmp, "x_input")
        x_output = os.path.join(tmp, "x_output")

        with open(x_input, "wb") as f:
            f.write(b"4")

        invoke(
            dag,
            argv=itertools.chain(
                *[
                    ["--input", "x", x_input],
                    [
                        "--output",
                        "x_doubled_and_squared",
                        x_output,
                    ],
                ]
            ),
        )

        with open(x_output, "rb") as f:
            assert f.read() == b"64"


def test__invoke__selecting_a_node_that_does_not_exist():
    dag = DAG(
        {
            "single-node": Task(lambda: 1),
        }
    )
    with pytest.raises(ValueError) as e:
        invoke(dag, argv=["--node-name", "missing-node"])

    assert (
        str(e.value)
        == "You selected node 'missing-node'. However, this DAG does not contain any node with such a name. These are the names the DAG contains: ['single-node']"
    )


def test__invoke__selecting_a_node():
    dag = DAG(
        nodes=dict(
            square=Task(
                lambda x: x ** 2,
                inputs=dict(x=FromParam()),
                outputs=dict(x_squared=FromReturnValue()),
            ),
        ),
        inputs=dict(x=FromParam()),
        outputs=dict(x_squared=FromNodeOutput("square", "x_squared")),
    )

    with tempfile.TemporaryDirectory() as tmp:
        x_input = os.path.join(tmp, "x_input")
        x_output = os.path.join(tmp, "x_output")

        with open(x_input, "wb") as f:
            f.write(b"4")

        invoke(
            dag,
            argv=itertools.chain(
                *[
                    ["--node-name", "square"],
                    ["--input", "x", x_input],
                    ["--output", "x_squared", x_output],
                ]
            ),
        )

        with open(x_output, "rb") as f:
            assert f.read() == b"16"


def test__invoke__selecting_a_node_from_nested_dag():
    dag = DAG(
        {
            "double": Task(
                lambda x: 2 * x,
                inputs=dict(x=FromParam()),
                outputs=dict(x=FromReturnValue()),
            ),
            "nested": DAG(
                {
                    "square": Task(
                        lambda x: x ** 2,
                        inputs=dict(x=FromParam()),
                        outputs=dict(x=FromReturnValue()),
                    ),
                },
                inputs=dict(x=FromParam()),
            ),
        },
        inputs=dict(x=FromParam()),
        outputs=dict(x=FromNodeOutput("double", "x")),
    )

    with tempfile.TemporaryDirectory() as tmp:
        x_input = os.path.join(tmp, "x_input")
        x_output = os.path.join(tmp, "x_output")

        with open(x_input, "wb") as f:
            f.write(b"4")

        invoke(
            dag,
            argv=itertools.chain(
                *[
                    ["--node-name", "nested.square"],
                    ["--input", "x", x_input],
                    ["--output", "x", x_output],
                ]
            ),
        )

        with open(x_output, "rb") as f:
            assert f.read() == b"16"


def test__invoke__selecting_a_nested_node_that_does_not_exist():
    dag = DAG(
        {
            "nested": DAG({"single-node": Task(lambda: 1)}),
        }
    )
    with pytest.raises(ValueError) as e:
        invoke(dag, argv=["--node-name", "nested.missing-node"])

    assert (
        str(e.value)
        == "You selected node 'nested.missing-node'. However, DAG 'nested' does not contain any node with such a name. These are the names the DAG contains: ['single-node']"
    )


def test__invoke__selecting_a_nested_dag():
    dag = DAG(
        {
            "nested": DAG(
                {
                    "square": Task(
                        lambda x: x ** 2,
                        inputs=dict(x=FromParam()),
                        outputs=dict(x=FromReturnValue()),
                    ),
                },
                inputs=dict(x=FromParam()),
                outputs=dict(x=FromNodeOutput("square", "x")),
            ),
        },
        inputs=dict(x=FromParam()),
    )

    with tempfile.TemporaryDirectory() as tmp:
        x_input = os.path.join(tmp, "x_input")
        x_output = os.path.join(tmp, "x_output")

        with open(x_input, "wb") as f:
            f.write(b"4")

        invoke(
            dag,
            argv=itertools.chain(
                *[
                    ["--node-name", "nested"],
                    ["--input", "x", x_input],
                    ["--output", "x", x_output],
                ]
            ),
        )

        with open(x_output, "rb") as f:
            assert f.read() == b"16"


def test__invoke__nested_node_with_inputs_from_another_node_output():
    non_default_serializer = AsPickle()
    dag = DAG(
        inputs={"x": FromParam()},
        outputs={"x": FromNodeOutput("l1-a", "x")},
        nodes={
            "l1-a": Task(
                lambda: 4,
                outputs={"x": FromReturnValue(serializer=non_default_serializer)},
            ),
            "l1-b": DAG(
                inputs={
                    "l1-b-y": FromNodeOutput(
                        "l1-a", "x", serializer=non_default_serializer
                    )
                },
                nodes={
                    "l2-a": Task(lambda: 3, outputs={"x": FromReturnValue()}),
                    "l2-b": Task(
                        lambda x, y: x * y,
                        inputs={
                            "x": FromNodeOutput("l2-a", "x"),
                            "y": FromParam("l1-b-y", serializer=non_default_serializer),
                        },
                        outputs={"x": FromReturnValue()},
                    ),
                },
            ),
        },
    )

    with tempfile.TemporaryDirectory() as tmp:
        x_input = os.path.join(tmp, "x_input")
        y_input = os.path.join(tmp, "y_input")
        x_output = os.path.join(tmp, "x_output")

        with open(x_input, "wb") as f:
            f.write(b"5")

        with open(y_input, "wb") as f:
            f.write(AsPickle().serialize(6))

        invoke(
            dag,
            argv=itertools.chain(
                *[
                    ["--node-name", "l1-b.l2-b"],
                    ["--input", "x", x_input],
                    ["--input", "y", y_input],
                    ["--output", "x", x_output],
                ]
            ),
        )

        with open(x_output, "rb") as f:
            assert f.read() == b"30"


def test__invoke__with_missing_input_parameter():
    dag = DAG(
        inputs={"x": FromParam()},
        nodes={"l1-a": Task(lambda: 1)},
    )
    with pytest.raises(ValueError) as e:
        invoke(dag, argv=["--input", "y", "f"])

    assert (
        str(e.value)
        == "This node is supposed to receive a pointer to an input named 'x'. However, only the following input pointers were supplied: ['y']"
    )


def test__invoke__with_missing_output_parameter():
    dag = DAG(
        outputs={"x": FromNodeOutput("n", "x")},
        nodes={"n": Task(lambda: 1, outputs={"x": FromReturnValue()})},
    )
    with pytest.raises(ValueError) as e:
        invoke(dag, argv=["--output", "y", "f"])

    assert (
        str(e.value)
        == "This node is supposed to receive a pointer to an output named 'x'. However, only the following output pointers were supplied: ['y']"
    )


def test__invoke__node_with_partitioned_output():
    dag = DAG(
        {
            "t": Task(
                lambda: [1, 2, 3],
                outputs={"list": FromReturnValue(is_partitioned=True)},
            ),
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        list_output = os.path.join(tmp, "list_output")

        invoke(
            dag,
            argv=itertools.chain(
                *[
                    ["--node-name", "t"],
                    ["--output", "list", list_output],
                ]
            ),
        )

        assert os.path.isdir(list_output)

        partitions = []
        with open(os.path.join(list_output, PARTITION_MANIFEST_FILENAME), "rb") as f:
            partition_filenames = json.load(f)

            for partition_filename in partition_filenames:
                with open(os.path.join(list_output, partition_filename), "rb") as p:
                    partitions.append(p.read())

        assert partitions == [b"1", b"2", b"3"]


def test__invoke__node_with_partitioned_input():
    dag = DAG(
        inputs={"partitioned": FromParam()},
        outputs={"together": FromNodeOutput("t", "together")},
        nodes={
            "t": Task(
                lambda partitioned: partitioned,
                inputs={"partitioned": FromParam()},
                outputs={"together": FromReturnValue()},
            ),
        },
    )

    with tempfile.TemporaryDirectory() as tmp:
        partitioned_input = os.path.join(tmp, "partitioned_input")
        together_output = os.path.join(tmp, "together_output")

        store_output_in_location(
            output_location=partitioned_input,
            output_value=PartitionedOutput([b"1", b"2", b"3"]),
        )
        assert os.path.isdir(partitioned_input)

        invoke(
            dag,
            argv=itertools.chain(
                *[
                    ["--input", "partitioned", partitioned_input],
                    ["--output", "together", together_output],
                ]
            ),
        )

        with open(together_output, "rb") as f:
            assert f.read() == b"[1, 2, 3]"
