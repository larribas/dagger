from typing import Annotated

import pytest

from dagger.dsl.serialize import Serialize, find_serialize_annotation
from dagger.serializer import AsJSON, AsPickle, DefaultSerializer

#
# Serialize
#


def test__serialize__default():
    serialize = Serialize()
    assert serialize.root == DefaultSerializer
    assert serialize.sub_output("missing") is None


def test__serialize__single_output():
    serialize = Serialize(AsPickle())
    assert serialize.root == AsPickle()
    assert serialize.sub_output("missing") is None


def test__serialize__root_output_and_others():
    serialize = Serialize(root=AsPickle(), a=AsJSON(indent=1), b=AsJSON(indent=3))
    assert serialize.root == AsPickle()
    assert serialize.sub_output("a") == AsJSON(indent=1)
    assert serialize.sub_output("b") == AsJSON(indent=3)
    assert serialize.sub_output("missing") is None


def test__serialize__multiple_sub_outputs_omitting_root():
    serialize = Serialize(a=AsJSON(indent=1), b=AsJSON(indent=3))
    assert serialize.root == DefaultSerializer
    assert serialize.sub_output("a") == AsJSON(indent=1)
    assert serialize.sub_output("b") == AsJSON(indent=3)
    assert serialize.sub_output("missing") is None


def test__serialize__representation():
    root_serializer = AsPickle()
    a_serializer = AsJSON(indent=1)
    b_serializer = AsJSON(indent=3)
    serialize = Serialize(
        root=root_serializer,
        a=a_serializer,
        b=b_serializer,
    )
    assert (
        repr(serialize)
        == f"Serialize(root={root_serializer}, a={a_serializer}, b={b_serializer})"
    )


#
# find_serialize_annotation
#


def test__find_serialize_annotation__when_there_are_no_annotated_types():
    def f():
        pass

    assert find_serialize_annotation(f) is None


def test__find_serialize_annotation__when_there_are_type_hints_without_annotations():
    def f() -> int:
        pass

    assert find_serialize_annotation(f) is None


def test__find_serialize_annotation__when_there_are_type_hints_with_unrelated_annotations():
    class UnrelatedAnnotation:
        pass

    def f() -> Annotated[int, UnrelatedAnnotation()]:
        pass

    assert find_serialize_annotation(f) is None


def test__find_serialize_annotation__when_there_is_a_serialize_annotation_for_a_single_output():
    serialize = Serialize(AsPickle())

    def f() -> Annotated[int, serialize]:
        pass

    assert find_serialize_annotation(f) == serialize


def test__find_serialize_annotation__when_there_is_a_serialize_annotation_for_multiple_outputs():
    serialize = Serialize(a=AsJSON(indent=1), b=AsPickle())

    def f() -> Annotated[int, serialize]:
        pass

    assert find_serialize_annotation(f) == serialize


def test__find_serialize_annotation__when_there_are_multiple_serialize_annotations():
    def f() -> Annotated[int, Serialize(), Serialize(AsPickle())]:
        pass

    with pytest.raises(ValueError) as e:
        find_serialize_annotation(f)

    assert (
        str(e.value)
        == "The return value of 'f' is annotated with multiple 'Serialize' annotations. This can lead to ambiguity about how to serialize its outputs, so we prefer to fail early and let you refactor your code. Please, remove this ambiguity."
    )
