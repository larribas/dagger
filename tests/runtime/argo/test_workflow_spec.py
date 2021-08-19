import pytest

from dagger.dag import DAG
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromReturnValue
from dagger.runtime.argo.errors import IncompatibilityError
from dagger.runtime.argo.workflow_spec import (
    Workflow,
    _dag_task_argument_artifact_from,
    _templates,
    workflow_spec,
)
from dagger.task import Task

#
# workflow_spec
#


def test__workflow_spec__simplest_dag():
    workflow = Workflow(
        container_image="my-image",
        container_entrypoint_to_dag_cli=["my", "dag", "entrypoint"],
    )
    dag = DAG(
        {
            "single-node": Task(lambda: 1),
        }
    )

    assert workflow_spec(dag, workflow) == {
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
                    "image": workflow.container_image,
                    "command": workflow.container_entrypoint_to_dag_cli,
                    "args": [
                        "--node-name",
                        "single-node",
                    ],
                },
            },
        ],
    }


def test__workflow_spec__nested_dags():
    workflow = Workflow(
        container_image="my-image",
        container_entrypoint_to_dag_cli=["my", "dag", "entrypoint"],
    )

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

    assert workflow_spec(dag, workflow) == {
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
                    "image": workflow.container_image,
                    "command": workflow.container_entrypoint_to_dag_cli,
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
                    "image": workflow.container_image,
                    "command": workflow.container_entrypoint_to_dag_cli,
                    "args": [
                        "--node-name",
                        "deeply.nested.a",
                    ],
                },
            },
        ],
    }


def test__workflow_spec__with_input_from_param_with_different_name():
    workflow = Workflow(
        container_image="my-image",
        container_entrypoint_to_dag_cli=["my", "dag", "entrypoint"],
        params={"a": 2},
    )

    dag = DAG(
        inputs=dict(a=FromParam()),
        outputs=dict(y=FromNodeOutput("times3", "x")),
        nodes=dict(
            times3=Task(
                lambda b: b * 3,
                inputs=dict(b=FromParam("a")),
                outputs=dict(x=FromReturnValue()),
            )
        ),
    )

    spec = workflow_spec(dag, workflow)

    assert spec["arguments"]["parameters"] == [{"name": "a", "value": 2}]
    assert spec["templates"][0]["dag"]["tasks"][0]["arguments"]["artifacts"] == [
        {
            "name": "b",
            "from": "{{inputs.artifacts.a}}",
        }
    ]


def test__workflow_spec__with_invalid_parameters():
    dag = DAG(
        nodes={
            "double": Task(
                lambda x: x * 2,
                inputs={"x": FromParam()},
                outputs={"2x": FromReturnValue()},
            ),
        },
        inputs={"x": FromParam()},
        outputs={"2x": FromNodeOutput(node="double", output="2x")},
    )

    with pytest.raises(ValueError) as e:
        workflow_spec(dag, Workflow(container_image="my-image"))

    assert (
        str(e.value)
        == "The parameters supplied to this DAG were supposed to contain the following parameters: ['x']. However, only the following parameters were actually supplied: []. We are missing: ['x']."
    )


def test__workflow_spec__with_template_overrides_that_affect_essential_attributes__fails():
    dag = DAG(
        {
            "n": Task(
                lambda: 1,
                runtime_options={
                    "argo_template_overrides": {
                        "container": {},
                        "name": "x",
                    }
                },
            )
        }
    )

    with pytest.raises(ValueError) as e:
        workflow_spec(dag, Workflow(container_image="my-image"))

    assert (
        str(e.value)
        == "In n, you are trying to override the value of ['container', 'name']. The Argo runtime uses these attributes to guarantee the behavior of the supplied DAG is correct. Therefore, we cannot let you override them."
    )


def test__workflow_spec__with_container_overrides_that_affect_essential_attributes__fails():
    dag = DAG(
        {
            "n": Task(
                lambda: 1,
                runtime_options={
                    "argo_container_overrides": {
                        "image": "x",
                    }
                },
            )
        }
    )

    with pytest.raises(ValueError) as e:
        workflow_spec(dag, Workflow(container_image="my-image"))

    assert (
        str(e.value)
        == "In n, you are trying to override the value of ['image']. The Argo runtime uses these attributes to guarantee the behavior of the supplied DAG is correct. Therefore, we cannot let you override them."
    )


def test__workflow_spec__with_task_overrides():
    container_image = "my-image"
    options = {
        "argo_template_overrides": {
            "timeout": "31m",
            "retryStrategy": {
                "limit": 11,
            },
            "priority": 3,
        },
        "argo_container_overrides": {
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
    }
    dag = DAG(
        {
            "n": Task(
                lambda: 1,
                runtime_options=options,
            )
        }
    )

    assert workflow_spec(dag, Workflow(container_image=container_image)) == {
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


def test__workflow_spec__with_dag_template_overrides_that_affect_essential_attributes__fails():
    dag = DAG(
        {"n": Task(lambda: 1)},
        runtime_options={
            "argo_dag_template_overrides": {
                "tasks": [],
            },
        },
    )

    with pytest.raises(ValueError) as e:
        workflow_spec(dag, Workflow(container_image="my-image"))

    assert (
        str(e.value)
        == "In this DAG, you are trying to override the value of ['tasks']. The Argo runtime uses these attributes to guarantee the behavior of the supplied DAG is correct. Therefore, we cannot let you override them."
    )


def test__workflow_spec__with_dag_template_overrides():
    container_image = "my-image"
    options = {
        "argo_dag_template_overrides": {
            "failFast": False,
            "extraAttribute": "extra",
        },
    }
    dag = DAG(
        {"n": Task(lambda: 1)},
        runtime_options=options,
    )

    assert workflow_spec(dag, Workflow(container_image=container_image)) == {
        "entrypoint": "dag",
        "templates": [
            {
                "name": "dag",
                "dag": {
                    "failFast": False,
                    "extraAttribute": "extra",
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


def test__workflow_spec__with_workflow_spec_overrides():
    workflow = Workflow(
        container_image="my-image",
        extra_spec_options={"suspend": True, "parallelism": 2},
    )
    dag = DAG({"n": Task(lambda: 1)})

    assert workflow_spec(dag, workflow) == {
        "entrypoint": "dag",
        "suspend": True,
        "parallelism": 2,
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
                    "image": workflow.container_image,
                    "args": ["--node-name", "n"],
                },
            },
        ],
    }


#
# dag_task_argument_artifact_from
#


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


#
# templates
#


def test__templates__with_incompatible_node():
    class IncompatibleNode:
        pass

    with pytest.raises(IncompatibilityError) as e:
        _templates(
            node=IncompatibleNode(),
            container_image="my-image",
            container_command=["my", "command"],
            params={"x": 1},
        )

    assert (
        str(e.value)
        == "Whoops. This node is of type 'IncompatibleNode'. While this node type may be supported by the DAG, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
    )


def test__templates__with_incompatible_node_and_address():
    class IncompatibleNode:
        pass

    with pytest.raises(IncompatibilityError) as e:
        _templates(
            node=IncompatibleNode(),
            container_image="my-image",
            container_command=["my", "command"],
            params={"x": 1},
            address=["some", "node"],
        )

    assert (
        str(e.value)
        == "Whoops. Node 'some.node' is of type 'IncompatibleNode'. While this node type may be supported by the DAG, the current version of the Argo runtime does not support it. Please, check the GitHub project to see if this issue has already been reported and addressed in a newer version. Otherwise, please report this as a bug in our GitHub tracker. Sorry for the inconvenience."
    )
