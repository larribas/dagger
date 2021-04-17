from argo_workflows_sdk import DAG, Node


def say_hello_world():
    print("Hello world")


dag = DAG(
    nodes={
        "say-hello-world": Node(say_hello_world),
    },
)
