from dagger import dsl


@dsl.task(
    runtime_options={
        "argo_template_overrides": {
            # Available options to override: https://argoproj.github.io/argo-workflows/fields/#template
            "retryStrategy": {"limit": 3},
            "activeDeadlineSeconds": 300,
        },
    }
)
def say_hello():
    print("Hello!")
