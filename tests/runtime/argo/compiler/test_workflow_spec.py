import pytest

import dagger.input as input
import dagger.output as output
from dagger.dag import DAG, DAGOutput
from dagger.runtime.argo.compiler.errors import IncompatibilityError
from dagger.runtime.argo.compiler.workflow_spec import (
    _dag_task_argument_artifact_from,
    workflow_spec,
)
from dagger.runtime.argo.options import ArgoTaskOptions
from dagger.task import Task


def test__workflow_spec__simplest_dag():
    container_image = "my-image"
    container_entrypoint = ["my", "dag", "entrypoint"]
    dag = DAG(
        {
            "single-node": Task(lambda: 1),
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
                            "template": "dag-single-node",
                        },
                    ],
                },
            },
            {
                "name": "dag-single-node",
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


def test__workflow_spec__nested_dags():
    container_image = "my-image"
    container_entrypoint = ["my", "dag", "entrypoint"]

    dag = DAG(
        {
            "a": Task(lambda: 1),
            "deeply": DAG(
                {
                    "nested": DAG(
                        {
                            "a": Task(lambda: 1),
                        }
                    )
                }
            ),
        },
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
                            "name": "a",
                            "template": "dag-a",
                        },
                        {
                            "name": "deeply",
                            "template": "dag-deeply",
                        },
                    ],
                },
            },
            {
                "name": "dag-a",
                "container": {
                    "image": container_image,
                    "command": container_entrypoint,
                    "args": [
                        "--node-name",
                        "a",
                    ],
                },
            },
            {
                "name": "dag-deeply",
                "dag": {
                    "tasks": [
                        {
                            "name": "nested",
                            "template": "dag-deeply-nested",
                        },
                    ],
                },
            },
            {
                "name": "dag-deeply-nested",
                "dag": {
                    "tasks": [
                        {
                            "name": "a",
                            "template": "dag-deeply-nested-a",
                        },
                    ],
                },
            },
            {
                "name": "dag-deeply-nested-a",
                "container": {
                    "image": container_image,
                    "command": container_entrypoint,
                    "args": [
                        "--node-name",
                        "deeply.nested.a",
                    ],
                },
            },
        ],
    }


def test__workflow_spec__with_invalid_parameters():
    dag = DAG(
        nodes={
            "double": Task(
                lambda x: x * 2,
                inputs={"x": input.FromParam()},
                outputs={"2x": output.FromReturnValue()},
            ),
        },
        inputs={"x": input.FromParam()},
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
    dag = DAG({"single-node": Task(lambda: 1)})

    assert (
        workflow_spec(
            dag,
            container_image="my-image",
            service_account=service_account,
        )["serviceAccountName"]
        == service_account
    )


def test__workflow_spec__with_empty_task_options():
    container_image = "my-image"
    options = ArgoTaskOptions()
    dag = DAG(
        {
            "n": Task(
                lambda: 1,
                runtime_options=[options],
            )
        }
    )

    assert workflow_spec(dag, container_image=container_image) == {
        "entrypoint": "dag",
        "templates": [
            {
                "name": "dag",
                "dag": {
                    "tasks": [
                        {
                            "name": "n",
                            "template": "dag-n",
                        },
                    ],
                },
            },
            {
                "name": "dag-n",
                "container": {
                    "image": container_image,
                    "args": ["--node-name", "n"],
                },
            },
        ],
    }


def test__workflow_spec__with_more_than_one_task_option__fails():
    dag = DAG(
        {
            "n": Task(
                lambda: 1,
                runtime_options=[
                    ArgoTaskOptions(),
                    ArgoTaskOptions(),
                ],
            )
        }
    )

    with pytest.raises(ValueError) as e:
        workflow_spec(dag, container_image="my-image")

    assert (
        str(e.value)
        == "You have specified two different instances of ArgoTaskOptions in task 'n'. This behavior may be ambiguous and not what you intended. Therefore, we prefer raising an exception here. If you really want to specify multiple sets of options (say, because you have generic and specific options and you want to keep your code DRY), we recommend you use Python functions."
    )


def test__workflow_spec__with_template_overrides_that_affect_essential_attributes__fails():
    dag = DAG(
        {
            "n": Task(
                lambda: 1,
                runtime_options=[
                    ArgoTaskOptions(
                        template_overrides={"container": {}, "name": "x"},
                    )
                ],
            )
        }
    )

    with pytest.raises(ValueError) as e:
        workflow_spec(dag, container_image="my-image")

    assert (
        str(e.value)
        == "In task 'n', you are trying to override the value of ['container', 'name']. The Argo runtime uses these attributes to guarantee the behavior of the supplied DAG is correct. Therefore, we cannot let you override them."
    )


def test__workflow_spec__with_container_overrides_that_affect_essential_attributes__fails():
    dag = DAG(
        {
            "n": Task(
                lambda: 1,
                runtime_options=[ArgoTaskOptions(container_overrides={"image": "x"})],
            )
        }
    )

    with pytest.raises(ValueError) as e:
        workflow_spec(dag, container_image="my-image")

    assert (
        str(e.value)
        == "In task 'n', you are trying to override the value of ['image']. The Argo runtime uses these attributes to guarantee the behavior of the supplied DAG is correct. Therefore, we cannot let you override them."
    )


def test__workflow_spec__with_specific_task_options():
    container_image = "my-image"
    options = ArgoTaskOptions(
        template_overrides={
            "timeout": "31m",
            "retryStrategy": {
                "limit": 11,
            },
            "priority": 3,
        },
        container_overrides={
            "resources": {
                "requests": {
                    "cpu": "500m",
                    "ephemeral-storage": "40Gi",
                },
                "limits": {
                    "memory": "8Gi",
                },
            },
        },
    )
    dag = DAG(
        {
            "n": Task(
                lambda: 1,
                runtime_options=[options],
            )
        }
    )

    assert workflow_spec(dag, container_image=container_image) == {
        "entrypoint": "dag",
        "templates": [
            {
                "name": "dag",
                "dag": {
                    "tasks": [
                        {
                            "name": "n",
                            "template": "dag-n",
                        },
                    ],
                },
            },
            {
                "name": "dag-n",
                "container": {
                    "image": container_image,
                    "args": ["--node-name", "n"],
                    "resources": {
                        "requests": {
                            "cpu": "500m",
                            "ephemeral-storage": "40Gi",
                        },
                        "limits": {
                            "memory": "8Gi",
                        },
                    },
                },
                "timeout": "31m",
                "retryStrategy": {
                    "limit": 11,
                },
                "priority": 3,
            },
        ],
    }


def test__dag_task_argument_artifact_from__with_incompatible_input():
    class IncompatibleInput:
        pass

    with pytest.raises(IncompatibilityError) as e:
        _dag_task_argument_artifact_from(
            node_address=["my", "nested", "node"],
            input_name="my-input",
            input=IncompatibleInput(),
        )

    assert (
        str(e.value)
        == "Whoops. Input 'my-input' of node 'my.nested.node' is of type 'IncompatibleInput'. While this input type may be supported by the DAG, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
    )
