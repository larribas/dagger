from argo_workflows_sdk import DAG, Node


def say_hello_world():
    print("Hello world")


dag = DAG(
    "hello-world",
    [
        Node("say-hello-world", say_hello_world),
    ],
)
