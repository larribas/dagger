import pytest

from dagger.data_structures.frozen_mapping import FrozenMapping


def test__setting_a_new_value__fails():
    error_message = "msg"
    frozen_dict = FrozenMapping(
        mapping={},
        error_message=error_message,
    )
    with pytest.raises(TypeError) as e:
        frozen_dict["new"] = "value"

    assert str(e.value) == error_message


def test__deleting_a_value__fails():
    error_message = "msg"
    frozen_dict = FrozenMapping(
        mapping={"a": 1},
        error_message=error_message,
    )
    with pytest.raises(TypeError) as e:
        del frozen_dict["a"]

    assert str(e.value) == error_message


def test__can_be_used_as_a_mapping():
    frozen_dict = FrozenMapping(
        mapping={"a": 1, "b": 2},
        error_message="",
    )

    assert "a" in frozen_dict
    assert frozen_dict["a"] == 1
    assert list(frozen_dict) == ["a", "b"]
    assert frozen_dict.get("a") == 1
    assert frozen_dict.get("x", 3) == 3
    assert list(frozen_dict.keys()) == ["a", "b"]
    assert list(frozen_dict.values()) == [1, 2]
    assert list(frozen_dict.items()) == [("a", 1), ("b", 2)]
    assert frozen_dict == {"a": 1, "b": 2}


def test__equality():
    a = FrozenMapping({"a": 1, "b": 2}, error_message="")
    b = FrozenMapping({"a": 1, "b": 2}, error_message="")
    c = FrozenMapping({"a": 2, "b": 2}, error_message="")

    assert a == b
    assert a != c


def test__equality_towards_other_types_of_mapping():
    a = FrozenMapping({"a": 1, "b": 2}, error_message="")
    assert a == {"a": 1, "b": 2}
