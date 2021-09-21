from typing import Any, List, Mapping

from dagger import dsl

DEFAULT_CONFIG_MAPS = ["default"]
DEFAULT_SECRETS = ["default"]


def argo_task(
    cpu_units: float,
    memory_in_gigabytes: float,
    timeout_in_minutes: int,
    max_retries: int = 0,
    env_from_config_maps: List[str] = DEFAULT_CONFIG_MAPS,
    env_from_secrets: List[str] = DEFAULT_SECRETS,
    serializer: dsl.Serialize = dsl.Serialize(),
    extra_container_options: Mapping[str, Any] = None,
    extra_template_options: Mapping[str, Any] = None,
):
    """
    Decorate a specific function as a task.

    This decorator extends `dagger.dsl.task` and provides an interface that:
    - Enforces the specification of a timeout, cpu and memory resources.
    - Allows injecting custom config maps and secrets as environment variables.
    - Adds custom metrics to record the duration of the task.

    Parameters
    ----------
    cpu_units
        Units of CPU the task will require during execution.

    memory_in_gigabytes
        GiB of memory the task will require during execution.

    timeout_in_minutes
        Maximum minutes the task is expected to run. After the task times out, it will be killed by Argo.

    max_retries
        The number of times the task will be retried before considering it "failed"

    env_from_config_maps
        The names of resources of type 'ConfigMap' to inject to the task.
        All the keys in the config maps will be exposed as environment variables.

    env_from_secrets
        The names of resources of type 'Secret' to inject to the task.
        All the keys in the config maps will be exposed as environment variables.

    serializer
        The number of times the task will be retried before considering it "failed"

    extra_container_options
        Arbitrary options to pass to the Argo template container (see https://argoproj.github.io/argo-workflows/fields/#container)

    extra_template_options
        Arbitrary options to pass to the Argo template (see https://argoproj.github.io/argo-workflows/fields/#template)


    Returns
    -------
    A decorated function that can be invoked with the same parameters as the original function
    """
    assert cpu_units > 0
    assert memory_in_gigabytes > 0
    assert timeout_in_minutes > 0
    assert max_retries >= 0

    extra_container_options = extra_container_options or {}
    extra_template_options = extra_template_options or {}

    argo_container_overrides = {
        "resources": {
            "requests": {
                "cpu": str(cpu_units),
                "memory": f"{memory_in_gigabytes}Gi",
            },
        },
        "envFrom": [
            *[
                {"configMapRef": {"name": config_map_name}}
                for config_map_name in env_from_config_maps
            ],
            *[{"secretRef": {"name": secret_name}} for secret_name in env_from_secrets],
        ],
        **extra_container_options,
    }
    argo_template_overrides = {
        "activeDeadlineSeconds": timeout_in_minutes * 60,
        "retryStrategy": {
            "limit": max_retries,
        },
        **extra_template_options,
    }

    return dsl.task(
        serializer=serializer,
        runtime_options={
            "argo_container_overrides": argo_container_overrides,
            "argo_template_overrides": argo_template_overrides,
        },
    )
