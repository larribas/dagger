import pytest

import dagger.inputs as inputs
import dagger.outputs as outputs
from dagger import Node
from dagger.runtime.local.node import invoke
from dagger.serializers import SerializationError

#
# Invoke
#


def test__invoke__without_inputs_or_outputs():
    invocations = []
    node = Node(lambda: invocations.append(1))
    assert invoke(node) == {}
    assert invocations == [1]


def test__invoke__with_single_input_and_output():
    node = Node(
        lambda number: number * 2,
        inputs=dict(number=inputs.FromParam()),
        outputs=dict(doubled_number=outputs.FromReturnValue()),
    )
    assert invoke(node, params=dict(number=b"2")) == dict(doubled_number=b"4")


def test__invoke__with_multiple_inputs_and_outputs():
    node = Node(
        lambda first_name, last_name: dict(
            message=f"Hello {first_name} {last_name}",
            name_length=len(first_name) + len(last_name),
        ),
        inputs=dict(
            first_name=inputs.FromParam(),
            last_name=inputs.FromParam(),
        ),
        outputs=dict(
            message=outputs.FromKey("message"),
            name_length=outputs.FromKey("name_length"),
        ),
    )
    assert invoke(node, params=dict(first_name=b'"John"', last_name=b'"Doe"',)) == dict(
        message=b'"Hello John Doe"',
        name_length=b"7",
    )


def test__invoke__with_missing_input_parameter():
    node = Node(lambda a: 1, inputs=dict(a=inputs.FromParam()))
    with pytest.raises(ValueError) as e:
        invoke(node, params={})

    assert (
        str(e.value)
        == "The parameters supplied to this node were supposed to contain a parameter named 'a', but only the following parameters were actually supplied: []"
    )


def test__invoke__with_mismatched_outputs():
    node = Node(lambda: 1, outputs=dict(a=outputs.FromKey("x")))
    with pytest.raises(TypeError) as e:
        invoke(node, params={})

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this node: This output is of type FromKey. This means we expect the return value of the function to be a dictionary containing, at least, a key named 'x'"
    )


def test__invoke__with_missing_outputs():
    node = Node(lambda: dict(a=1), outputs=dict(x=outputs.FromKey("x")))
    with pytest.raises(ValueError) as e:
        invoke(node, params={})

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this node: An output of type FromKey('x') expects the return value of the function to be a dictionary containing, at least, a key named 'x'"
    )


def test__invoke__with_unserializable_outputs():
    node = Node(lambda: dict(a=lambda: 2), outputs=dict(x=outputs.FromKey("a")))
    with pytest.raises(SerializationError) as e:
        invoke(node, params={})

    assert (
        str(e.value)
        == "We encountered the following error while attempting to serialize the results of this node: Object of type function is not JSON serializable"
    )
