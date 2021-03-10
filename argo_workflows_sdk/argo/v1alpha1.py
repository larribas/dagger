from typing import List

from argo_workflows_sdk import DAG

API_VERSION = "argoproj.io/v1alpha1"


def as_workflow(
    dag: DAG,
    name_prefix: str,
    namespace: str,
    service_account: str,
    container_image: str,
    container_dag_entrypoint: List[str],
):

    return {
        "apiVersion": API_VERSION,
        "kind": "Workflow",
        "metadata": {
            "generateName": name_prefix,
            "namespace": namespace,
        },
        "spec": {
            "entrypoint": "main",
            "serviceAccountName": service_account,
            "templates": [
                {"name": "main", "dag": {"tasks": _dag_tasks(dag)}},
                *_node_templates(
                    dag,
                    container_image=container_image,
                    container_command=container_dag_entrypoint,
                ),
            ],
        },
    }


def _node_templates(
    dag: DAG,
    container_image: str,
    container_command: List[str],
):
    return [
        {
            "name": node.name,
            "container": {
                "image": container_image,
                "command": container_command,
                "args": [f"--node-name={node.name}"],
            },
        }
        for node in dag.nodes
    ]


def _dag_tasks(dag: DAG):
    return [
        {
            "name": node.name,
            "template": node.name,
            "dependencies": [],
        }
        for node in dag.nodes
    ]
