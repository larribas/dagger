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
