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
    spec = {**original}

    for key_to_override, overridden_value in extra_options.items():
        this_context = ".".join([context, key_to_override])

        is_valid_type = any(
            [isinstance(overridden_value, t) for t in [int, str, float, dict, list]]
        )
        if overridden_value is not None and not is_valid_type:
            raise ValueError(
                f"You are trying to set '{this_context}' to a value of type '{type(overridden_value).__name__}'. However, only values with a primitive type (None, int, float, str, dict or list) are accepted."
            )

        if key_to_override not in spec:
            spec[key_to_override] = overridden_value
        elif isinstance(spec[key_to_override], list) and isinstance(
            overridden_value, list
        ):
            spec[key_to_override] += overridden_value
        elif isinstance(spec[key_to_override], dict) and isinstance(
            overridden_value, dict
        ):
            spec[key_to_override] = with_extra_spec_options(
                original=spec[key_to_override],
                extra_options=overridden_value,
                context=this_context,
            )
        else:
            raise ValueError(
                f"You are trying to override the value of '{this_context}'. The Argo runtime already sets a value for this key, and it uses it to guarantee the correctness of the behavior. Therefore, we cannot let you override them."
            )

    return spec
