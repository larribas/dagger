from argo_workflows_sdk import DAG, Node
from argo_workflows_sdk.argo import as_workflow


def say_hello_world():
    print("Hello world")


dag = DAG(
    [
        Node("say_hello_world", say_hello_world),
    ],
)
