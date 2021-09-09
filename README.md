# Dagger

Define sophisticated data pipelines and run them on different distributed systems (such as Argo Workflows).

![Python Versions Supported](https://img.shields.io/badge/python-3.8+-blue.svg)
[![Latest PyPI version](https://badge.fury.io/py/py-dagger.svg)](https://badge.fury.io/py/py-dagger)
[![Test Coverage (Codecov)](https://codecov.io/gh/larribas/dagger/branch/main/graph/badge.svg?token=fKU68xYUm8)](https://codecov.io/gh/larribas/dagger)
![QA: Tests](https://github.com/larribas/dagger/actions/workflows/tests.yaml/badge.svg)
![QA: Documentation](https://github.com/larribas/dagger/actions/workflows/documentation.yaml/badge.svg)
![QA: Type System](https://github.com/larribas/dagger/actions/workflows/linting.yaml/badge.svg)
![QA: Formatting](https://github.com/larribas/dagger/actions/workflows/formatting.yaml/badge.svg)

---

_Dagger_ is a Python library that allows you to:

* Define sophisticated DAGs (direct acyclic graphs) using very straightforward Pythonic code.
* Run those DAGs seamlessly in different runtimes or workflow orchestrators (such as Argo Workflows, Kubeflow Pipelines, and more).


## Features

- Define tasks and DAGs, and compose them together seamlessly.
- Parameterize DAGs and pass parameters between nodes in plain Python (the runtime takes care of serializing and transmitting data on your behalf).
- Create dynamic for loops, map-reduce operations easily.
- Run your DAGs locally or using a distributed workflow orchestrator (such as Argo Workflows).
- Extend your tasks to take advantage of all the features offered by your runtime (e.g. Retry strategies, Kubernetes scheduling directives, etc.)
- ... All with a simple _Pythonic_ DSL that feels just like coding regular Python functions.


Other nice features of _Dagger_ are: Zero dependencies, 100% test coverage, great documentation and plenty of examples to get you started.


## Installation

_Dagger_ is published to the Python Package Index (PyPI) under the name `py-dagger`. To install it, you can simply run:

```
pip install py-dagger
```


## Overview

_Dagger_ was created to facilitate the creation and ongoing maintenance of data and ML pipelines at big companies.

This goal is reflected in _Dagger_'s architecture and main design decisions:

- To make __common use cases__ and patterns (such as dynamic loops or map-reduce operations) __as easy as possible__.
- To __minimize boilerplate, plumbing or low-level code__ (such as serializing inputs/outputs and storing them in a local/remote file system).
- To __onboard users in just a couple of hours__ through great documentation, comprehensive examples and tutorials.
- To __never sacrifice reliability and performance__.


Take the following piece of code:

```python
import random
from dagger import dsl

@dsl.task()
def generate_numbers():
    length = random.randint(3, 20)
    numbers = list(range(length))
    print(f"Generating the following list of numbers: {numbers}")
    return numbers

@dsl.task()
def raise_number(n, exponent):
    print(f"Raising {n} to a power of {exponent}")
    return n ** exponent

@dsl.task()
def sum_numbers(numbers):
    print(f"Calculating the sum of {numbers}")
    return sum(numbers)

@dsl.DAG()
def map_reduce_pipeline(exponent):
    numbers = generate_numbers()

    raised_numbers = []
    for n in numbers:
      raised_numbers.append(
        raise_number(n, exponent)
      )

    return sum_numbers(raised_numbers)

dag = dsl.build(map_reduce_pipeline)

from dagger.runtime.local import invoke
result = invoke(dag, params={"exponent": 2})
print(f"The final result was {result}")
```

Let's go step by step. First, we use the `dagger.dsl.task` decorator to define different tasks. Tasks in _Dagger_ are just Python functions. In this case, we have 3 tasks:

- `generate_numbers()` returns a list of numbers. The list has a variable length, to show how we can do dynamic loops.
- `raise_number(n, exponent)` receives a number and an exponent, and returns `n^exponent`.
- `sum_numbers(numbers)` receives a list of numbers and returns the sum of all of them.

Next, we use the `dagger.dsl.DAG` decorator on another function that invokes all the previously defined tasks and connects their inputs/outputs.

The example uses a for loop and appends elements to a list to gather all the different results. But you can try replacing it with something more _Pythonic_:

```python
@dsl.DAG
def map_reduce_pipeline(exponent):
    return sum_numbers([raise_number(n, exponent) for n in generate_numbers()])
```

Finally, we use `dagger.dsl.build` to transform that decorated function into a `dagger.dag.DAG` data structure, and we test it locally with `dagger.runtime.local.invoke`.

```python
from dagger.runtime.local import invoke
result = invoke(dag, params={"exponent": 2})
print(f"The final result was {result}")
```


### Other Runtimes

The previous example showed how we can model a fairly complex use case (a dynamic map-reduce) and run it locally in just a few lines of code.

The great thing about _Dagger_ is that running your pipeline in a distributed pipeline engine (such as Argo Workflows or Kubeflow Pipelines) is just as easy!

At the moment, we support the following runtimes:

- `dagger.runtime.local` for local experimentation and testing.
- `dagger.runtime.cli`, used by other runtimes.
- `dagger.runtime.argo`, to run your pipelines on [Argo Workflows](https://argoproj.github.io/workflows/).


You can check the [Runtimes Documentation](user-guide/runtimes/alternatives.md) to get started with any of them.


### Tutorials, Examples and User Guides

Does it sound interesting? We're just scratching the surface of what's possible with _Dagger_. If you're interested, you can begin exploring:

- The [Tutorial](tutorial/introduction.md) for a step-by-step introduction to _Dagger_.
- The [User Guide](user-guide/introduction.md) for an in-depth explanation of all the different components available, extensibility points and design decisions.
- The [API Reference](api/init.md).
- The [Examples](https://github.com/larribas/dagger/tree/main/examples).


## How to contribute

Do you have some feedback about the library? Have you implemented a Serializer or a Runtime that may be useful for the community? Do you think a tutorial or example could be improved?

Every contribution to _Dagger_ is greatly appreciated.

Please read our [Contribution Guidelines](CONTRIBUTING.md) for more details.



### Local development

We use Poetry to manage the dependencies of this library. In the codebase, you will find a `Makefile` with some useful commands to run and test your contributions. Namely:

- `make install` - Install the project's dependencies
- `make test` - Run tests and report test coverage. It will fail if coverage is too low.
- `make ci` - Run all the quality checks we run for each commit/PR. This includes type hint checking, linting, formatting and documentation.
- `make build` - Build the project's WHEEL
- `make docker-build` - Package the project in a Docker image
- `make k3d-set-up` - Create a k3d cluster and image registry for the project.
- `make k3d-docker-push` - Build and push the project's Docker image to the local k3d registry.
- `make k3d-install-argo` - Install Argo on k3d, for local testing of Argo Workflows.
- `make k3d-tear-down` - Destroy the k3d cluster and registry.
