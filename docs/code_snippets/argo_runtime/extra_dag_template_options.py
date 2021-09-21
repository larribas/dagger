from dagger import dsl


@dsl.task()
def say_hello():
    print("Hello!")


@dsl.DAG(
    runtime_options={
        "argo_dag_template_overrides": {
            # Available options to override: https://argoproj.github.io/argo-workflows/fields/#dagtemplate
            "failFast": False,
        },
    }
)
def dag():
    say_hello()
