# Dagger

![tests](https://github.com/larribas/dagger/actions/workflows/tests.yaml/badge.svg) ![documentation](https://github.com/larribas/dagger/actions/workflows/documentation.yaml/badge.svg) ![type system](https://github.com/larribas/dagger/actions/workflows/linting.yaml/badge.svg) ![style](https://github.com/larribas/dagger/actions/workflows/formatting.yaml/badge.svg)

---

Dagger is a library that allows you to:

* Define sophisticated DAGs (direct acyclic graphs) using very straightforward Python code.
* Run those DAGs seamlessly in different runtimes or workflow orchestrators (such as Argo Workflows, Kubeflow Pipelines or Airflow)


_Please check the Roadmap section below to understand what is supported and what is not at the moment_


## Motivation and Design Principles

The main goal of this library is to provide a __simple yet powerful framework__ to define data/ML pipelines with __minimal friction or boilerplate__ and __run them on any number of execution runtimes__.

It was built following these principles:

* Provide a __high-level abstraction__ that hides repetitive or

* Reduce the friction of passing inputs and outputs across different steps of the workflow.
* Allow users to take full advantage of all of Argo's features (exit handlers, memoization, timeouts, retries, conditional executions, and so on)
* Enable platform teams to enforce global standards and features on all workflows.



## Current status

At this point, we're exploring different APIs and abstractions. The project is still unusable.


## Development

Useful commands:

- `make install` - Install the project's dependencies
- `make build` - Build the project's WHEEL
- `make docker-build` - Package the project in a Docker image
- `make docker-run-example-name` - Run any example DAG of those defined in "argo_workflows_sdk/examples" (for instance, `make run-hello-world`)
- `make docker-push-local` - Build and push the project's Docker image to the local k3d registry.
- `make set-up-argo` - Creates a k3d cluster for the project and installs Argo 3.0 in it.
- `make tear-down-argo` - Destroys the k3d cluster and registry where we deployed Argo.
