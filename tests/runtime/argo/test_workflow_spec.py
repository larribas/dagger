import pytest

import dagger.inputs as inputs
import dagger.outputs as outputs
from dagger.dag import DAG, DAGOutput
from dagger.node import Node
from dagger.runtime.argo.errors import IncompatibilityError
from dagger.runtime.argo.workflow_spec import (
    dag_task_argument_artifact_from,
    workflow_spec,
)


def test__workflow_spec__simplest_dag():
    container_image = "my-image"
    container_entrypoint = ["my", "dag", "entrypoint"]
    dag = DAG(
        {
            "single-node": Node(lambda: 1),
        }
    )

    assert workflow_spec(
        dag,
        container_image=container_image,
        container_entrypoint_to_dag_cli=container_entrypoint,
    ) == {
        "entrypoint": "dag",
        "templates": [
            {
                "name": "dag",
                "dag": {
                    "tasks": [
                        {
                            "name": "single-node",
                            "template": "node-single-node",
                        },
                    ],
                },
            },
            {
                "name": "node-single-node",
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
    }


def test__workflow_spec__with_invalid_parameters():
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
        workflow_spec(dag, container_image="my-image")

    assert (
        str(e.value)
        == "The parameters supplied to this DAG were supposed to contain a parameter named 'x', but only the following parameters were actually supplied: []"
    )


def test__workflow_spec__setting_service_account():
    service_account = "my-service-account"
    dag = DAG({"single-node": Node(lambda: 1)})

    assert (
        workflow_spec(
            dag,
            container_image="my-image",
            service_account=service_account,
        )["serviceAccountName"]
        == service_account
    )


def test__dag_task_argument_artifact_from__with_incompatible_input():
    class IncompatibleInput:
        pass

    with pytest.raises(IncompatibilityError) as e:
        dag_task_argument_artifact_from(
            node_name="my-node", input_name="my-input", input=IncompatibleInput()
        )

    assert (
        str(e.value)
        == "Whoops. Input 'my-input' of node 'my-node' is of type 'IncompatibleInput'. While this input type may be supported by the DAG, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
    )
