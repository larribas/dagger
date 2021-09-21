from dagger import dsl


@dsl.task(
    runtime_options={
        "argo_container_overrides": {
            # Available options to override: https://argoproj.github.io/argo-workflows/fields/#container
            "resources": {
                "requests": {
                    "cpu": "16",
                    "memory": "64Gi",
                },
            },
        },
    }
)
def say_hello():
    print("Hello!")
