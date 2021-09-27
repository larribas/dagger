# Argo Runtime

The Argo runtime allows you to run your DAGs on [_Argo Workflows_](https://argoproj.github.io/workflows/).

In their words:

> Argo Workflows is an open source container-native workflow engine for orchestrating parallel jobs on Kubernetes. Argo Workflows is implemented as a Kubernetes CRD.


What this means is that _Argo_ will create a handful of [Custom Kubernetes Resources](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/): `Workflow`, `CronWorkflow` and `WorkflowTemplate`, among others.

Then, it will expect workflows to be expressed declaratively (usually using YAML), following the [specification](https://argoproj.github.io/argo-workflows/fields/) of those resources.

Here are [some examples](https://github.com/argoproj/argo-workflows/tree/7684ef4a0c5f57e8723dc8e4d3a17246f7edc2e6/examples) that show how `Workflow` manifests look like.

__The responsibility of the Argo runtime is to generate those manifests__.

## 📜 Generating Manifests

The Argo runtime exposes a series of methods to generate manifests for the custom resources mentioned above.

This is how you would generate different resource manifests using the Argo runtime:


=== "Workflow"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/workflow.py"
    ```

=== "Cron Workflow"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/cron_workflow.py"
    ```

=== "Workflow Template"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/workflow_template.py"
    ```

=== "Cluster Workflow Template"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/cluster_workflow_template.py"
    ```


### Why do we need container images and entrypoints

_Argo Workflows_ will execute each of the tasks in your DAG using a container.


This requires you to build a container image that can run the DAG's tasks.

Building container images is out of the scope of this guide, but the official [Python Docker Image](https://hub.docker.com/_/python) provides some useful examples.

Once you've built a container, you need to specify which entrypoint of that container is able to execute the DAG through the command-line interface provided by the [CLI runtime](cli.md).

Here is an example project that defines a DAG, exposes it through the CLI runtime, specifies how to build a container image using a Dockerfile, and finally has a script that generates the Argo manifests and dumps them into a YAML file.

=== "my_pipeline.py"

    ```python
    --8<-- "docs/code_snippets/cli_runtime/imperative.py"
    ```

=== "Dockerfile"

    ```docker
    FROM python:3

    WORKDIR /usr/src/app

    COPY requirements.txt ./
    RUN pip install --no-cache-dir -r requirements.txt

    COPY . .

    ENTRYPOINT [ "python" ]
    ```

=== "generate_manifests.py"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/generate_manifests.py"
    ```



## 🔧 Runtime options

Many of Argo's features are not first-class citizens in _Dagger_. For instance:

- _Dagger_ doesn't understand that tasks may have timeouts or retry strategies.
- _Dagger_ doesn't understand that tasks may have resource requests or limits.
- _Dagger_ doesn't understand that you may want to fine-tune how your tasks are scheduled in your _Kubernetes_ cluster using node selectors, tolerations or affinities.

Nevertheless, ___Dagger_ allows you to set all of these settings together with the behavior of your task__, so you don't lose your head doing complex post-processing of the manifests, or defining all these options in a separate configuration file.


=== "Container options"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/extra_container_options.py"
    ```

=== "Task options"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/extra_task_options.py"
    ```

=== "Task Template options"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/extra_template_options.py"
    ```

=== "DAG Template options"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/extra_dag_template_options.py"
    ```

=== "Workflow options"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/extra_workflow_options.py"
    ```

=== "Cron Workflow options"

    ```python
    --8<-- "docs/code_snippets/argo_runtime/extra_cron_workflow_options.py"
    ```




## 👮 Enforcing your Company's Conventions and Standards

The Argo runtime allows you to specify arbitrary options for many of the elements in the `Workflow` specification.

However, when implementing _Dagger_ inside of a large corporation, __platform teams may want to enforce certain conventions and standards to comply with the company's governance, cost, observability or compliance policies__.

The following example shows how easy it is to extend the existing decorators to provide a more opinionated API and ease the day-to-day of both platform teams and _Dagger_ users in the organization.

```python
--8<-- "docs/code_snippets/argo_runtime/syntax_sugar.py"
```

This example was taken from [Glovo](https://glovoapp.com/), which has been the first company to adopt _Dagger_.



## 📗 API Reference

Check the [API Reference](../../api/runtime-argo.md) for more details about this runtime and the options each of the methods accept.
