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


## _Dagger_ in Action

This section shows a couple of examples of what _Dagger_ is capable of. Our [official documentation](https://dagger.readthedocs.io) contains a breadth of tutorials, examples, recommendations and API references. Make sure to check it out!


### Installing the library

_Dagger_ is published to the Python Package Index (PyPI). To install it, you can simply run:

```
pip install py-dagger
```


### Hello World - Tasks and DAGs

The following example shows how to run a simple hello world using the local runtime:


```python
from dagger.dsl import task, DAG, build
from dagger.runtime.local import invoke

@task
def say_hello_world():
    print("hello world!")

@DAG
def hello_world_pipeline():
    say_hello_world()

dag = build(hello_world_pipeline)
invoke(dag)
```

Running this will print `"hello world!"`.

While not particularly interesting, this example shows the basic building blocks of _DAGger_: __Tasks__ and __DAGs (directed acyclic graphs)__. DAGs contain a series of nodes connected together via their inputs/outputs. Nodes may be Tasks (a Python function wrapped with some extra metadata) or other DAGs.

It also shows how we can define DAGs in an imperative, Pythonic style, build them (i.e. turning them into a data structure representing the DAG) and run them using one of our runtimes (in this case, the local runtime, which will just run it in memory).

Hungry for more? Let's take a look at a more complex example.


### Map-Reduce Operations - Parameters and parallelization

The following example generates a list of numbers. The length of the list varies randomly. Then, in parallel, we transform/map each of these numbers raising them to a power we receive as a parameter. Finally, we sum all the results and produce a single output.


```python
import random

@task
def generate_numbers():
    length = random.randint(3, 20)
    numbers = list(range(length))
    print(f"Generating the following list of numbers: {numbers}")
    return numbers

@task
def raise_number(n, exponent):
    print(f"Raising {n} to a power of {exponent}")
    return n ** exponent

@task
def sum_numbers(numbers):
    print(f"Calculating the sum of {numbers}")
    return sum(numbers)

@DAG
def map_reduce_pipeline(exponent):
    return sum_numbers(
        [
            raise_number(n=partition, exponent=exponent)
            for partition in generate_numbers()
        ]
    )

dag = build(map_reduce_pipeline)
result = invoke(dag, params={"exponent": 2})
print(f"The final result was {result}")
```


This type of parallel fan-out and fan-in operations are very common when modelling data pipelines. _Dagger_ allows you to write them as you would in plain Python and run them on a number of distributed systems.


### Built-in and Custom Serializers

One of the things you may notice is that the result of running the DAG is not of type `int`, but of type `bytes`. This is because a node produces results in their __serialized format__. This may look like an odd choice when running things locally, but keep in mind that the final goal of the library is to be able to run each step of your DAG in different machines over the network.

_Dagger_ helps you connect the outputs of some nodes to the inputs of other nodes, but to do so, these pieces of information have to travel as bytes through the network, and be serialized/deserialized when leaving/entering the Python code.

__The default serialization format is JSON__, but in practice, you will find yourself using other kinds of serialization formats that fit your use case better.

For instance, when dealing with Pandas `DataFrame`s, you may want to serialize those data frames as CSV or Parquet files. You can do that easily via type annotations:

```python
# ...
import pandas as pd
from typing import Dict
from dagger.dsl import task, Serialize
from my_custom_serializers import AsParquet

@task(serializer=Serialize(training=AsParquet(), testing=AsParquet()))
def split_training_test_datasets(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    # ...
    return {
      "training": training_dataset,
      "testing": testing_dataset,
    }

# ...
```

Your function code just works with Python data types, but under the hood, _Dagger_ is using the serializers you provide to pass information from one node to the other.

You can check the serializers we provide out of the box in our documentation, under [Serializers](TODO). And you can bring your own serializers very easily.



### Running your DAG on a distributed runtime

So far, we have run our DAGs using the `runtime.local` package. Next, you can try exporting your DAG for execution in any of the following runtimes:

* [Argo Workflows](TODO)



## Design Principles and Features

The main goal of _Dagger_ is to provide a __simple yet powerful framework__ to define data/ML pipelines with __minimal friction or boilerplate__ and __prepare them to run on multiple distributed runtimes__.

The features the library provides are based on these 3 guiding principles:


### 1. High-level abstraction for the most common use cases; Extensibility for more specific use cases

_Dagger_ provides out-of-the-box support for common patterns such as:

- Passing arguments from one node to another.
- Parameterizing a DAG so its behavior can change dynamically when it's executed.
- Running multiple nodes in parallel.
- Encapsulating common behavior and composing several DAGs together.
- Creating map-reduce operations (also known as "scatter and gather" or "dynamic fan-out and fan-in" operations).


_Dagger_ also removes the need to explicitly serialize/deserialize data, or interface with local or remote filesystems. Users write Python code and deal with Python-native data types. That's it.

We believe the majority of the use cases can be built using the existing feature set. However, we also believe users should still be able to __leverage the features that make each runtime unique__. For instance:

* When running on Kubernetes, users may want to fine-tune some scheduling directives.
* When running on Argo Workflows, users may want to set a retry policy, or use memoization on their steps.
* When developing data pipelines, users may want to use Parquet as a serialization format.
* Users may want to create a new runtime that is not yet supported by the library.

_Dagger_ is designed for extensibility. When you need to take your DAGs to the next step, you will not find yourself fighting against the framework.


### 2. Soft learning curve

We believe if you are already fluent with Python, you should be able to pick up _Dagger_ in a couple of hours.

To soften the learning curve, we've worked hard on:

* A [documentation portal](https://dagger.readthedocs.io) with tutorials, recommendations and API references.
* A comprehensive [set of examples](https://github.com/larribas/dagger/tree/main/examples), from beginner to advanced use cases.
* Thorough error handling to catch any potential issue as early as possible. Error messages are descriptive, point you to the specific component that is causing a problem, explain the reason why it's failing and suggest alternatives.



### 3. Performant and reliable

_Dagger_ enables you to create and iterate on complex workflows. During this effort, the library should never be a limiting factor in terms of performance or reliability. That is, we want to make sure you don't experience any bugs, memory leaks or conflicts that impair your productivity. Hence, we have put a lot of focus on:

- __Test coverage__ for internal components. _Dagger_ will always have >95% test coverage for all success and error scenarios.
- __Zero dependencies__. When you install _Dagger_, it doesn't bring any other dependency with it. Your requirements file will be clean and conflict-free with other versions of other libraries.
- __Lazy loading of input files__. Where possible, _Dagger_ will minimize the memory footprint by using lazy loading of files from local or remote filesystems into memory. This is especially useful when dealing with partitioned outputs and reduce operations.
- __Local verification__ of your DAGs. When you build a DAG, we enforce a series of rules that make your pipelines clear and predictable. You can also execute any of your DAGs locally with the local runtime.


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
