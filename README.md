# argo-workflows-sdk

The purpose of this project is to provide a set of abstractions to define complex workflows and run them in Argo Workflows.


## Design goals

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
