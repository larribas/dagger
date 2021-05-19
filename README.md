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
- `make build` - Build the project's WHEEL and a docker image around it
- `make run-example-name` - Run any example DAG of those defined in "argo_workflows_sdk/examples" (for instance, `make run-hello-world`)
- `make set-up-argo` - Creates a k3d cluster for the project and installs Argo 3.0 in it.
- `make push-local` - Build and push the project's Docker image to the local k3d registry.
- `make tear-down-argo` - Destroys the k3d cluster and registry where we deployed Argo.


## Roadmap

- [ ] Support passing inputs/outputs as parameters, and infer dependencies automatically based on those
- [x] Support multiple outputs in a function (via dictionaries or dataclasses)
- [ ] Add black + isort + flake8 to the codebase
- [ ] Run a Continuous Integration workflow via GitHub Actions
- [ ] Support DAG inputs/outputs
- [ ] Support passing inputs/outputs as artifacts
- [ ] Support timeouts
- [ ] Support composite DAGs
- [ ] Support failFast in DAGs
- [ ] Support exit handlers
- [ ] Support retry policies
- [ ] Support setting resource requests/limits
- [ ] Support conditional executions
- [ ] Support dynamic fan-outs
- [ ] Support dynamic fan-ins
- [ ] Support recursion
- [ ] Support custom metrics
