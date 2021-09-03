import pytest

from dagger.runtime.argo.extra_spec_options import with_extra_spec_options


def test__with_extra_options__with_empty_overrides():
    original = {"a": 1}
    assert with_extra_spec_options(original, {}, "my context") == original


def test__with_extra_options__overriding_existing_attributes():
    original = {"z": 2, "a": 1, "b": 3}

    with pytest.raises(ValueError) as e:
        with_extra_spec_options(original, {"z": 4, "a": 2}, "my context")

    assert (
        str(e.value)
        == "In my context, you are trying to override the value of ['a', 'z']. The Argo runtime uses these attributes to guarantee the behavior of the supplied DAG is correct. Therefore, we cannot let you override them."
    )


def test__with_extra_options__setting_extra_options():
    original = {"a": 1}
    assert with_extra_spec_options(original, {"b": 2, "c": 3}, "my context") == {
        "a": 1,
        "b": 2,
        "c": 3,
    }
