# argo-workflows-sdk

The purpose of this project is to provide a set of abstractions to define complex workflows and run them in Argo Workflows.


## Design goals

* Reduce the friction of passing inputs and outputs across different steps of the workflow.
* Allow users to take full advantage of all of Argo's features (exit handlers, memoization, timeouts, retries, conditional executions, and so on)
* Enable platform teams to enforce global standards and features on all workflows.



## Current status

At this point, we're exploring different APIs and abstractions. The project is still unusable.


## Useful commands

- `make install` - Install the project's dependencies
- `make build` - Build the project's WHEEL and a docker image around it
- `make run-hello-world` - Run the example hello world pipeline
- `make run-example-name` - Run any example DAG of those defined in `argo_workflows_sdk/examples`
