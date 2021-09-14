import pytest

from dagger.runtime.argo.extra_spec_options import with_extra_spec_options


def test__with_extra_options__with_empty_overrides():
    original = {"a": 1}
    assert with_extra_spec_options(original, {}, "my context") == original


def test__with_extra_options__with_invalid_values():
    class InvalidValue:
        pass

    with pytest.raises(ValueError) as e:
        with_extra_spec_options(
            {"x": {}},
            {"x": {"y": InvalidValue()}},
            "my context",
        )

    assert (
        str(e.value)
        == "You are trying to set 'my context.x.y' to a value of type 'InvalidValue'. However, only values with a primitive type (None, int, float, str, dict or list) are accepted."
    )


def test__with_extra_options__with_deep_overrides():
    original = {
        "a": {
            "a": 1,
            "b": [1],
        },
    }
    assert with_extra_spec_options(
        original,
        {
            "a": {
                "b": [2],
                "c": 2,
            },
        },
        "my context",
    ) == {
        "a": {
            "a": 1,
            "b": [1, 2],
            "c": 2,
        }
    }


def test__with_extra_options__overriding_existing_attributes():
    original = {"z": 2, "a": {"a": 1}, "b": 3}

    with pytest.raises(ValueError) as e:
        with_extra_spec_options(original, {"y": 3, "a": {"a": 2}}, "my context")

    assert (
        str(e.value)
        == "You are trying to override the value of 'my context.a.a'. The Argo runtime already sets a value for this key, and it uses it to guarantee the correctness of the behavior. Therefore, we cannot let you override them."
    )


def test__with_extra_options__setting_extra_options_that_didnt_exist():
    original = {"a": 1}
    assert with_extra_spec_options(original, {"b": 2, "c": 3}, "my context") == {
        "a": 1,
        "b": 2,
        "c": 3,
    }
