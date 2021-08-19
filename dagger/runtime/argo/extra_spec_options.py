"""
Function to allow overriding attributes on the spec of a manifest.

This is a design decision to give dagger users the option to set overrides at different levels:
- The container
- The template
- The DAG
- The Workflow
- The CronWorkflow
- ...

Taking advantage of any arbitrary features added to the spec without having to update this library.

Overrides are only allowed if the Argo runtime doesn't use those keys for a a specific purpose.

At the time, a basic key replacement is performed. Strategic patches could be an option in the future if we need greater flexibility.
"""

from typing import Any, Mapping


def with_extra_spec_options(
    original: Mapping[str, Any],
    extra_options: Mapping[str, Any],
    context: str,
) -> Mapping[str, Any]:
    """
    Given an original arbitrary spec and a set of overrides, verify the overrides don't intersect with the existing attributes and return both mappings merged.

    Parameters
    ----------
    original
        Original map of key-values.

    extra_options
        Options to set.

    context
        Context in which this options are being set. This is used to produce a useful error message.


    Raises
    ------
    ValueError
        If we attempt to override keys that are already present in the original mapping.
    """
    if not extra_options:
        return original

    key_intersection = set(extra_options).intersection(original)
    if key_intersection:
        raise ValueError(
            f"In {context}, you are trying to override the value of {sorted(list(key_intersection))}. The Argo runtime uses these attributes to guarantee the behavior of the supplied DAG is correct. Therefore, we cannot let you override them."
        )

    return {**original, **extra_options}
