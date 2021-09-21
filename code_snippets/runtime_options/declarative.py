from dagger import DAG, FromNodeOutput, FromParam, FromReturnValue, Task


def query_database(query):
    return "query results..."


def process_data(data):
    return "processed results"


dag = DAG(
    inputs={
        "query": FromParam(),
    },
    outputs={
        "results": FromNodeOutput("process-data", "data"),
    },
    nodes={
        "query-database": Task(
            query_database,
            inputs={
                "query": FromParam("query"),
            },
            outputs={
                "data": FromReturnValue(),
            },
            runtime_options={
                "argo_template_overrides": {
                    "retryStrategy": {"limit": 3},
                    "activeDeadlineSeconds": 300,
                },
            },
        ),
        "process-data": Task(
            process_data,
            inputs={
                "data": FromNodeOutput("query-database", "data"),
            },
            outputs={
                "data": FromReturnValue(),
            },
            runtime_options={
                "argo_container_overrides": {
                    "resources": {
                        "requests": {
                            "cpu": "16",
                            "memory": "64Gi",
                        },
                    },
                },
            },
        ),
    },
    runtime_options={
        "argo_dag_template_overrides": {
            "failFast": False,
        },
    },
)
