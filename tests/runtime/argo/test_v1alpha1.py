import pytest

import argo_workflows_sdk.inputs as inputs
import argo_workflows_sdk.outputs as outputs
from argo_workflows_sdk.dag import DAG, DAGOutput
from argo_workflows_sdk.node import Node
from argo_workflows_sdk.runtime.argo.v1alpha1 import workflow_manifest

#
# Workflow manifest
#


def test__workflow_manifest__simplest_dag():
    workflow_name = "my-workflow"
    container_image = "my-image"
    container_entrypoint = ["my-dag-entrypoint"]
    dag = DAG(
        {
            "single-node": Node(lambda: 1),
        }
    )

    assert workflow_manifest(
        dag,
        name=workflow_name,
        container_image=container_image,
        container_entrypoint_to_dag_cli=container_entrypoint,
    ) == {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Workflow",
        "metadata": {
            "name": workflow_name,
        },
        "spec": {
            "entrypoint": "main",
            "templates": [
                {
                    "name": "main",
                    "dag": {
                        "tasks": [
                            {
                                "name": "single-node",
                                "template": "single-node",
                            },
                        ],
                    },
                },
                {
                    "name": "single-node",
                    "container": {
                        "image": container_image,
                        "command": container_entrypoint,
                        "args": [
                            "--node-name",
                            "single-node",
                        ],
                    },
                },
            ],
        },
    }


def test__workflow_manifest__with_invalid_parameters():
    workflow_name = "my-workflow"
    container_image = "my-image"
    container_entrypoint = ["my-dag-entrypoint"]
    dag = DAG(
        nodes={
            "double": Node(
                lambda x: x * 2,
                inputs={"x": inputs.FromParam()},
                outputs={"2x": outputs.FromReturnValue()},
            ),
        },
        inputs={"x": inputs.FromParam()},
        outputs={"2x": DAGOutput(node="double", output="2x")},
    )

    with pytest.raises(ValueError) as e:
        workflow_manifest(
            dag,
            name=workflow_name,
            container_image=container_image,
            container_entrypoint_to_dag_cli=container_entrypoint,
        )

    assert (
        str(e.value)
        == "The parameters supplied to this DAG were supposed to contain a parameter named 'x', but only the following parameters were actually supplied: []"
    )


def test__workflow_manifest__setting_namespace():
    namespace = "my-namespace"
    dag = DAG({"single-node": Node(lambda: 1)})

    assert (
        workflow_manifest(
            dag,
            name="my-workflow",
            container_image="my-image",
            container_entrypoint_to_dag_cli=[],
            namespace=namespace,
        )["metadata"]["namespace"]
        == namespace
    )


def test__workflow_manifest__setting_annotations():
    annotations = {
        "my.annotation/one": "1",
        "my.annotation/two": "2",
    }
    dag = DAG({"single-node": Node(lambda: 1)})

    assert (
        workflow_manifest(
            dag,
            name="my-workflow",
            container_image="my-image",
            container_entrypoint_to_dag_cli=[],
            annotations=annotations,
        )["metadata"]["annotations"]
        == annotations
    )


def test__workflow_manifest__setting_labels():
    labels = {
        "my.label/one": "1",
        "my.label/two": "2",
    }
    dag = DAG({"single-node": Node(lambda: 1)})

    assert (
        workflow_manifest(
            dag,
            name="my-workflow",
            container_image="my-image",
            container_entrypoint_to_dag_cli=[],
            labels=labels,
        )["metadata"]["labels"]
        == labels
    )


def test__workflow_manifest__setting_service_account():
    service_account = "my-service-account"
    dag = DAG({"single-node": Node(lambda: 1)})

    assert (
        workflow_manifest(
            dag,
            name="my-workflow",
            container_image="my-image",
            container_entrypoint_to_dag_cli=[],
            service_account=service_account,
        )["spec"]["serviceAccountName"]
        == service_account
    )


def test__workflow_manifest__generating_name_from_prefix():
    name_prefix = "my-name-prefix-"
    dag = DAG({"single-node": Node(lambda: 1)})

    manifest = workflow_manifest(
        dag,
        name=name_prefix,
        generate_name_from_prefix=True,
        container_image="my-image",
        container_entrypoint_to_dag_cli=[],
    )

    assert "name" not in manifest["metadata"]
    assert manifest["metadata"]["generateName"] == name_prefix
