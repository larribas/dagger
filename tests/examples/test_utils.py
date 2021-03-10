from pathlib import Path

import yaml
from deepdiff import DeepDiff


def load_yaml(name: str):
    example_yamls_path = Path(__file__).parent
    with open(f"{example_yamls_path}/{name}.yaml", "r") as f:
        return yaml.load(f)


def assert_deep_equal(a, b):
    """
    Compare the supplied data strcture against each other, and assert their equality.
    The motivation for this function is to produce a succint summary of the differences between both structures as an assertion message.
    """
    diff = DeepDiff(a, b, ignore_order=True)
    assert not diff
