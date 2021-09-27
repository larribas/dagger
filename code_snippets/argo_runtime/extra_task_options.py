from dagger import dsl


@dsl.task(
    runtime_options={
        "argo_task_overrides": {
            # Available options to override: https://argoproj.github.io/argo-workflows/fields/#dagtask
            "continueOn": {
                "failed": True,
            },
        },
    }
)
def say_hello():
    print("Hello!")
