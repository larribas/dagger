import warnings
from itertools import combinations

import pytest

from dagger.dag import DAG, CyclicDependencyError, validate_parameters
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromReturnValue
from dagger.serializer import AsPickle, DefaultSerializer, SerializationError
from dagger.task import Task

#
# Init
#


def test__init__with_no_nodes():
    with pytest.raises(ValueError) as e:
        DAG(nodes={})

    assert str(e.value) == "A DAG needs to contain at least one node"


def test__init__with_an_invalid_node_name():
    invalid_names = [
        "",
        "name with spaces",
        "x" * 65,
        "with$ymßols",
        "with_underscores",
    ]

    for name in invalid_names:
        with pytest.raises(ValueError) as e:
            DAG(
                nodes={name: Task(lambda: 1)},
            )

        assert (
            str(e.value)
            == f"'{name}' is not a valid name for a node. Node names must comply with the regex ^[a-zA-Z0-9][a-zA-Z0-9-]{{0,63}}$"
        )


def test__init__with_a_valid_node_name():
    valid_names = [
        "param",
        "name-with-dashes",
        "name-with-dashes-and-123",
        "x" * 64,
    ]

    for name in valid_names:
        # We are testing it doesn't raise any validation errors
        DAG(
            nodes={name: Task(lambda: 1)},
        )


def test__init__with_an_invalid_input_name():
    with pytest.raises(ValueError):
        DAG(
            nodes={"my-node": Task(lambda: 1)},
            inputs={"invalid name": FromParam()},
        )


def test__init__with_invalid_input_type():
    class UnsupportedInput:
        def __init__(self):
            self.serializer = DefaultSerializer

    with pytest.raises(TypeError) as e:
        DAG(
            nodes=dict(
                n=Task(
                    lambda x: x,
                    inputs=dict(x=FromParam()),
                ),
            ),
            inputs=dict(x=UnsupportedInput()),
        )

    assert (
        str(e.value)
        == "Input 'x' is of type 'UnsupportedInput'. However, DAGs only support the following types of inputs: ['FromParam', 'FromNodeOutput']"
    )


def test__init__with_an_invalid_output_name():
    with pytest.raises(ValueError):
        DAG(
            nodes={"my-node": Task(lambda: 1, outputs=dict(x=FromReturnValue()))},
            outputs={"invalid name": FromNodeOutput("my-node", "x")},
        )


def test__init__with_an_output_that_references_a_nonexistent_node():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={"my-node": Task(lambda: 1)},
            outputs=dict(x=FromNodeOutput("missing-node", "y")),
        )

    assert (
        str(e.value)
        == "Output 'x' depends on the output of a node named 'missing-node'. However, the DAG does not contain any node with such a name. These are the nodes contained by the DAG: ['my-node']"
    )


def test__init__with_an_output_that_references_a_nonexistent_node_output():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={"my-node": Task(lambda: 1, outputs=dict(x=FromReturnValue()))},
            outputs=dict(y=FromNodeOutput("my-node", "z")),
        )

    assert (
        str(e.value)
        == "Output 'y' depends on the output 'z' of another node named 'my-node'. However, node 'my-node' does not declare any output with such a name. These are the outputs defined by the node: ['x']"
    )


def test__init__with_two_outputs_referencing_the_same_node_output():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={"my-node": Task(lambda: 1, outputs=dict(x=FromReturnValue()))},
            outputs=dict(
                x1=FromNodeOutput("my-node", "x"),
                x2=FromNodeOutput("my-node", "x"),
            ),
        )

    assert (
        str(e.value)
        == "Multiple DAG outputs depend on the same node output. This is not a valid pattern in dagger due to the ambiguity and potential problems it may cause."
    )


def test__init__with_a_node_that_references_another_that_does_not_exist():
    with pytest.raises(ValueError) as e:
        DAG(
            {
                "my-node": Task(
                    lambda x: 1,
                    inputs=dict(x=FromNodeOutput("missing-node", "x")),
                ),
            }
        )

    assert (
        str(e.value)
        == "Error validating input 'x' of node 'my-node': This input depends on the output of another node named 'missing-node'. However, the DAG does not define any node with such a name. These are the nodes contained by the DAG: ['my-node']"
    )


def test__init__with_a_node_that_references_an_output_that_does_not_exist():
    with pytest.raises(ValueError) as e:
        DAG(
            {
                "first-node": Task(
                    lambda: 1,
                    outputs=dict(z=FromReturnValue()),
                ),
                "second-node": Task(
                    lambda x: 1,
                    inputs=dict(x=FromNodeOutput("first-node", "y")),
                ),
            }
        )

    assert (
        str(e.value)
        == "Error validating input 'x' of node 'second-node': This input depends on the output 'y' of another node named 'first-node'. However, node 'first-node' does not declare any output with such a name. These are the outputs defined by the node: ['z']"
    )


def test__init__with_mismatched_inputs_from_node_output():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={
                "first-node": Task(
                    lambda: 1,
                    outputs=dict(x=FromReturnValue(serializer=AsPickle())),
                ),
                "second-node": Task(
                    lambda x: 1,
                    inputs=dict(x=FromNodeOutput("first-node", "x")),
                ),
            },
        )

    assert (
        str(e.value)
        == "Error validating input 'x' of node 'second-node': This input is serialized AsJSON(indent=None, allow_nan=False). However, the output it references is serialized AsPickle()."
    )


def test__init__with_a_node_that_references_a_dag_input_that_does_not_exist():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={
                "my-node": Task(
                    lambda x: 1,
                    inputs=dict(x=FromParam()),
                ),
            },
            inputs=dict(z=FromParam()),
        )

    assert (
        str(e.value)
        == "Error validating input 'x' of node 'my-node': This input depends on a parameter named 'x' being injected into the DAG. However, the DAG does not have any parameter with such a name. These are the parameters the DAG receives: ['z']"
    )


def test__init__with_mismatched_inputs_from_param():
    with pytest.raises(ValueError) as e:
        DAG(
            nodes={
                "my-node": Task(
                    lambda x: 1,
                    inputs=dict(x=FromParam()),
                ),
            },
            inputs=dict(x=FromParam(serializer=AsPickle())),
        )

    assert (
        str(e.value)
        == "Error validating input 'x' of node 'my-node': This input is serialized AsJSON(indent=None, allow_nan=False). However, the input it references is serialized AsPickle()."
    )


def test__init__with_a_node_that_references_an_existing_dag_input_explicitly():
    DAG(
        nodes={
            "my-node": Task(
                lambda x: 1,
                inputs=dict(x=FromParam("z")),
            ),
        },
        inputs=dict(z=FromParam()),
    )
    # We are expecting no validation errors to be raised


def test__init__with_a_cyclic_dependency():
    with pytest.raises(CyclicDependencyError):
        DAG(
            nodes=dict(
                a=Task(
                    lambda x: x,
                    inputs=dict(x=FromNodeOutput("a", "x")),
                    outputs=dict(x=FromReturnValue()),
                ),
            ),
        )


def test__init__with_nested_dags():
    DAG(
        {
            "outermost": DAG(
                {
                    "come-up-with-a-number": Task(
                        lambda: 1, outputs=dict(x=FromReturnValue())
                    ),
                    "middle": DAG(
                        {
                            "innermost": Task(
                                lambda x: x,
                                inputs=dict(x=FromParam()),
                                outputs=dict(y=FromReturnValue()),
                            )
                        },
                        inputs=dict(x=FromNodeOutput("come-up-with-a-number", "x")),
                        outputs=dict(yy=FromNodeOutput("innermost", "y")),
                    ),
                },
                outputs=dict(yyy=FromNodeOutput("middle", "yy")),
            )
        },
        outputs=dict(yyyy=FromNodeOutput("outermost", "yyy")),
    )
    # Then, no validation exceptions are raised


def test__init__partitioned_by_nonexistent_input():
    with pytest.raises(ValueError) as e:
        DAG(
            inputs={
                "z": FromParam(),
                "a": FromParam(),
            },
            nodes={
                "x": Task(lambda: 1),
            },
            partition_by_input="b",
        )

    assert (
        str(e.value)
        == "This node is partitioned by 'b'. However, 'b' is not an input of the node. The available inputs are ['a', 'z']."
    )


def test__init__with_dag_output_from_a_partitioned_node():
    with pytest.raises(ValueError) as e:
        DAG(
            outputs={"r": FromNodeOutput("map", "n")},
            nodes={
                "fan-out": Task(
                    lambda: [1, 2, 3],
                    outputs={"n": FromReturnValue(is_partitioned=True)},
                ),
                "map": Task(
                    lambda n: n,
                    inputs={
                        "n": FromNodeOutput("fan-out", "n"),
                    },
                    outputs={
                        "n": FromReturnValue(),
                    },
                    partition_by_input="n",
                ),
            },
        )

    assert (
        str(e.value)
        == "Output 'r' comes from node 'map', which is partitioned. This is not a valid map-reduce pattern in dagger. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
    )


def test__init__partitioned_by_output_of_partitioned_node():
    with pytest.raises(ValueError) as e:
        DAG(
            {
                "fan-out": Task(
                    lambda: [1, 2],
                    outputs={"numbers": FromReturnValue(is_partitioned=True)},
                ),
                "map-1": Task(
                    lambda n: n,
                    inputs={"n": FromNodeOutput("fan-out", "numbers")},
                    outputs={"n": FromReturnValue()},
                    partition_by_input="n",
                ),
                "map-2": Task(
                    lambda n: n,
                    inputs={"n": FromNodeOutput("map-1", "n")},
                    outputs={"n": FromReturnValue()},
                    partition_by_input="n",
                ),
            }
        )

    assert (
        str(e.value)
        == "Error validating input 'n' of node 'map-2': This node is partitioned by an input that comes from the output of another partitioned node. This is not a valid map-reduce pattern in dagger. Please check the 'Map Reduce' section in the documentation for an explanation of why this is not possible and suggestions of other valid map-reduce patterns."
    )


#
# Node execution order
#


def test__node_execution_order__with_no_dependencies():
    dag = DAG(
        nodes=dict(
            one=Task(lambda: 1),
            two=Task(lambda: 2),
        ),
    )
    assert dag.node_execution_order == [{"one", "two"}]


def test__node_execution_order__is_based_on_dependencies():
    dag = DAG(
        nodes=dict(
            second=Task(
                lambda x: x * 2,
                inputs=dict(x=FromNodeOutput("first", "x")),
                outputs=dict(x=FromReturnValue()),
            ),
            third=Task(
                lambda x: x * 2,
                inputs=dict(x=FromNodeOutput("second", "x")),
            ),
            first=Task(
                lambda: 1,
                outputs=dict(x=FromReturnValue()),
            ),
        ),
    )
    assert dag.node_execution_order == [{"first"}, {"second"}, {"third"}]


#
# Properties
#


def test__inputs__cannot_be_mutated():
    dag = DAG(
        {"my-node": Task(lambda: 1)},
        inputs=dict(x=FromParam()),
    )

    with pytest.raises(TypeError) as e:
        dag.inputs["y"] = FromParam()

    assert (
        str(e.value)
        == "You may not mutate the inputs of a DAG after it has been initialized. We do this to guarantee the structures you build with dagger remain valid and consistent."
    )


def test__outputs__cannot_be_mutated():
    dag = DAG(
        {
            "my-node": Task(
                lambda: 1,
                outputs=dict(x=FromReturnValue()),
            )
        },
        outputs=dict(x=FromNodeOutput("my-node", "x")),
    )

    with pytest.raises(TypeError) as e:
        dag.outputs["x"] = FromNodeOutput("my-node", "y")

    assert (
        str(e.value)
        == "You may not mutate the outputs of a DAG after it has been initialized. We do this to guarantee the structures you build with dagger remain valid and consistent."
    )


def test__nodes__cannot_be_mutated():
    dag = DAG({"my-node": Task(lambda: 1)})

    with pytest.raises(TypeError) as e:
        del dag.nodes["my-node"]

    assert (
        str(e.value)
        == "You may not mutate the nodes of a DAG after it has been initialized. We do this to guarantee the structures you build with dagger remain valid and consistent."
    )


def test__runtime_options__is_empty_by_default():
    dag = DAG({"my-node": Task(lambda: 1)})
    assert len(dag.runtime_options) == 0


def test__runtime_options__returns_specified_options():
    options = {"my-runtime": {"my": "options"}}
    dag = DAG(
        {"my-node": Task(lambda: 1)},
        runtime_options=options,
    )
    assert dag.runtime_options == options


def test__partition_by_input():
    assert DAG({"my-node": Task(lambda: 1)}).partition_by_input is None
    assert (
        DAG(
            inputs={"a": FromParam()},
            nodes={"x": Task(lambda: 1)},
            partition_by_input="a",
        ).partition_by_input
        == "a"
    )


def test__eq():
    def f(**kwargs):
        return 11

    nodes = {"my-node": Task(lambda: 1, outputs=dict(x=FromReturnValue()))}
    inputs = dict(x=FromParam())
    outputs = dict(x=FromNodeOutput("my-node", "x"))
    runtime_options = {"my": "options"}

    same = [
        DAG(
            nodes=nodes,
            inputs=inputs,
            outputs=outputs,
            runtime_options=runtime_options,
        )
        for i in range(3)
    ]

    different = [
        DAG(
            nodes=nodes, inputs=inputs, outputs=outputs, runtime_options=runtime_options
        ),
        DAG(nodes=nodes, inputs=inputs, outputs=outputs),
        DAG(nodes=nodes, inputs=inputs, runtime_options=runtime_options),
        DAG(nodes=nodes, outputs=outputs, runtime_options=runtime_options),
        DAG(
            nodes={"my-node": Task(lambda: 2, outputs=dict(x=FromReturnValue()))},
            inputs=inputs,
            outputs=outputs,
            runtime_options=runtime_options,
        ),
    ]

    assert all(x == y for x, y in combinations(same, 2))
    assert all(x != y for x, y in combinations(different, 2))


#
# validate_parameters
#


def test__validate_parameters__when_params_match_inputs():
    validate_parameters(
        inputs={
            "a": FromParam(),
            "b": FromNodeOutput("n", "o"),
        },
        params={
            "a": 1,
            "b": "2",
        },
    )
    # We are testing there are no exceptions raised as a result of calling the validator


def test__validate_parameters__when_input_is_missing():
    with pytest.raises(ValueError) as e:
        validate_parameters(
            inputs={
                "c": FromParam(),
                "a": FromParam(),
                "b": FromNodeOutput("n", "o"),
            },
            params={
                "c": 1,
                "a": 1,
            },
        )

    assert (
        str(e.value)
        == "The parameters supplied to this DAG were supposed to contain the following parameters: ['a', 'b', 'c']. However, only the following parameters were actually supplied: ['a', 'c']. We are missing: ['b']."
    )


def test__validate_parameters__when_param_is_superfluous():
    with warnings.catch_warnings(record=True) as w:
        validate_parameters(
            inputs={
                "c": FromParam(),
                "a": FromParam(),
            },
            params={
                "z": 1,
                "a": 1,
                "c": 1,
                "y": 1,
            },
        )
        assert len(w) == 1
        assert (
            str(w[0].message)
            == "The following parameters were supplied to this DAG, but are not necessary: ['y', 'z']"
        )


def test__validate_parameters__when_param_value_is_not_compatible_with_serializer():
    with pytest.raises(SerializationError) as e:
        validate_parameters(
            inputs={
                "a": FromParam(),
            },
            params={
                "a": {1},
            },
        )

    assert (
        str(e.value)
        == "The value supplied for input 'a' is not compatible with the serializer defined for that input (AsJSON(indent=None, allow_nan=False)): Object of type set is not JSON serializable"
    )
