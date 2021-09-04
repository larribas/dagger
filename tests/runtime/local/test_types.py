from collections.abc import Iterable, Iterator

import pytest

from dagger.runtime.local.types import PartitionedOutput


def test__partitioned_output__is_iterable():
    output = PartitionedOutput([1, 2])
    assert isinstance(output, Iterable)
    assert isinstance(iter(output), Iterator)

    i = iter(output)
    assert next(i) == 1
    assert next(i) == 2

    with pytest.raises(StopIteration):
        next(i)


def test__partitioned_output__representation():
    assert repr(PartitionedOutput([1, 2, 3])) == "[1, 2, 3]"
