from dagger import dsl


@dsl.task(
    runtime_options={
        "argo_template_overrides": {
            "retryStrategy": {"limit": 3},
            "activeDeadlineSeconds": 300,
        },
    }
)
def query_database(query):
    return "query results..."


@dsl.task(
    runtime_options={
        "argo_container_overrides": {
            "resources": {
                "requests": {
                    "cpu": "16",
                    "memory": "64Gi",
                },
            },
        },
    }
)
def process_data(data):
    return "processed results"


@dsl.DAG(
    runtime_options={
        "argo_dag_template_overrides": {
            "failFast": False,
        },
    }
)
def dag(query):
    data = query_database(query)
    return process_data(data)
