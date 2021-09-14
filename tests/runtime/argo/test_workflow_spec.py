import pytest

from dagger.dag import DAG
from dagger.input import FromNodeOutput, FromParam
from dagger.output import FromReturnValue
from dagger.runtime.argo.workflow_spec import (
    Workflow,
    _dag_task_with_param,
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
                "inputs": {
                    "parameters": [
                        {"name": "name", "value": "dag"},
                    ],
                },
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
                "inputs": {
                    "parameters": [
                        {"name": "name", "value": "dag"},
                    ],
                },
                "dag": {
                    "tasks": [
                        {
                            "name": "a",
                            "template": "dag-a",
                        },
                        {
                            "name": "deeply",
                            "template": "dag-deeply",
                            "arguments": {
                                "parameters": [
                                    {
                                        "name": "name",
                                        "value": "{{inputs.parameters.name}}-deeply",
                                    },
                                ],
                            },
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
                "inputs": {
                    "parameters": [
                        {"name": "name"},
                    ],
                },
                "dag": {
                    "tasks": [
                        {
                            "name": "nested",
                            "template": "dag-deeply-nested",
                            "arguments": {
                                "parameters": [
                                    {
                                        "name": "name",
                                        "value": "{{inputs.parameters.name}}-nested",
                                    },
                                ],
                            },
                        },
                    ],
                },
            },
            {
                "name": "dag-deeply-nested",
                "inputs": {
                    "parameters": [
                        {"name": "name"},
                    ],
                },
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
                        "container": {
                            "image": "a-different-image",
                        },
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
        == "You are trying to override the value of 'n.container.image'. The Argo runtime already sets a value for this key, and it uses it to guarantee the correctness of the behavior. Therefore, we cannot let you override them."
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
        == "You are trying to override the value of 'n.image'. The Argo runtime already sets a value for this key, and it uses it to guarantee the correctness of the behavior. Therefore, we cannot let you override them."
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
                "inputs": {
                    "parameters": [
                        {"name": "name", "value": "dag"},
                    ],
                },
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
                "tasks": None,
            },
        },
    )

    with pytest.raises(ValueError) as e:
        workflow_spec(dag, Workflow(container_image="my-image"))

    assert (
        str(e.value)
        == "You are trying to override the value of 'DAG.tasks'. The Argo runtime already sets a value for this key, and it uses it to guarantee the correctness of the behavior. Therefore, we cannot let you override them."
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
                "inputs": {
                    "parameters": [
                        {"name": "name", "value": "dag"},
                    ],
                },
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
                "inputs": {
                    "parameters": [
                        {"name": "name", "value": "dag"},
                    ],
                },
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


def test__dag_task_with_param():
    assert (
        _dag_task_with_param("my-input", FromParam("parent-input"))
        == "{{workflow.parameters.parent-input}}"
    )
    assert (
        _dag_task_with_param("my-input", FromParam())
        == "{{workflow.parameters.my-input}}"
    )
    assert (
        _dag_task_with_param(
            "my-input", FromNodeOutput("another-node", "another-output")
        )
        == "{{tasks.another-node.outputs.parameters.another-output_partitions}}"
    )
