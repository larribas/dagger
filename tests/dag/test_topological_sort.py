import pytest

from argo_workflows_sdk.dag.topological_sort import (
    CyclicDependencyError,
    topological_sort,
)

#
# Topological Sort
#


def test__topological_sort__with_empty_dependencies():
    assert topological_sort({}) == []


def test__topological_sort__with_cyclic_dependency():
    cases = [
        dict(
            topology={1: {1}},
            nodes_involved_in_cycle={1},
        ),
        dict(
            topology={1: {2}, 2: {1}},
            nodes_involved_in_cycle={1, 2},
        ),
        dict(
            topology={1: {2}, 2: {3}, 3: {1}},
            nodes_involved_in_cycle={1, 2, 3},
        ),
        dict(
            topology={1: {2, 4}, 2: {3, 5}, 3: {4, 5, 2}},
            nodes_involved_in_cycle={1, 2, 3},
        ),
    ]

    for case in cases:
        with pytest.raises(CyclicDependencyError) as e:
            topological_sort(case["topology"])

        assert (
            str(e.value)
            == f"There is a cyclic dependency between the following nodes: {case['nodes_involved_in_cycle']}"
        )


def test__topological_sort__with_complex_topologies():
    cases = [
        dict(
            topology={
                1: {2},
                3: {4, 5},
            },
            right_order=[
                {2, 4, 5},
                {1, 3},
            ],
        ),
        dict(
            topology={
                2: {11},
                9: {11, 8},
                10: {11, 3},
                11: {7, 5},
                8: {7, 3},
            },
            right_order=[
                {3, 5, 7},
                {8, 11},
                {2, 9, 10},
            ],
        ),
    ]

    for case in cases:
        assert topological_sort(case["topology"]) == case["right_order"]
