# Dagger

Define sophisticated data pipelines and run them on different distributed systems (such as Argo Workflows).

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/larribas/dagger/blob/main/LICENSE.md) ![tests](https://github.com/larribas/dagger/actions/workflows/tests.yaml/badge.svg) ![documentation](https://github.com/larribas/dagger/actions/workflows/documentation.yaml/badge.svg) ![type system](https://github.com/larribas/dagger/actions/workflows/linting.yaml/badge.svg) ![style](https://github.com/larribas/dagger/actions/workflows/formatting.yaml/badge.svg)

---

_Dagger_ is a Python library that allows you to:

* Define sophisticated DAGs (direct acyclic graphs) using very straightforward Pythonic code.
* Run those DAGs seamlessly in different runtimes or workflow orchestrators (such as Argo Workflows, Kubeflow Pipelines or Airflow)


_Please check the Roadmap section below to understand what is supported and what is not at the moment_


## _Dagger_ in Action

This section shows a couple of examples of what _Dagger_ is capable of. Our [official documentation](TODO) contains a breadth of tutorials, examples, recommendations and API references. Make sure to check it out!


### Installing the library

_Dagger_ is published to the Python Package Index (PyPI). To install it, you can simply run:

```
pip install dagger
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

from typing import Annotated
from dagger.dsl import Serialize
from my_custom_serializers import DataFrameAsParquet

@task
def split_training_test_datasets(df: pd.DataFrame) -> Annotated[
    pd.DataFrame,
    Serialize(
        training=DataFrameAsParquet(),
        testing=DataFrameAsParquet(),
    ),
]:
    # ...
    return {
      "training": training_dataset,
      "testing": testing_dataset,
    }

# ...
```

Your function code just works with Python data types, but under the hood, _Dagger_ is using the serializers you provide to pass information from one node to the other.

You can check the serializers we provide out of the box in our documentation, under [Serializers](TODO). And you can bring your own serializers very easily.




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

* A [documentation portal](TODO) with tutorials, recommendations and API references.
* A comprehensive [set of examples](TODO), from beginner to advanced use cases.
* Thorough error handling to catch any potential issue as early as possible. Error messages are descriptive, point you to the specific component that is causing a problem, explain the reason why it's failing and suggest alternatives.



### 3. Performant and reliable

_Dagger_ is there to enable you to create 

be a lightweight and robust component of your project. This means:

- __>95% test coverage__.
- __Zero dependencies__.
- __DAGs can be executed locally__ to facilitate testing.

You shouldn't have any conflicts 

 You shouldn't experience any bugs or unexpected limitations. The codebase has >95% test coverage,.



* Lightweight and reliable
  * zero dependencies
  * Reliability (heavily tested codebase and testable user code through DAG inspection and execution).
  * Declarative definition of DAGs (DAGs are immutable, composable data structures)
  * Declarative definition of DAGs (DAGs are immutable, composable data structures)






## How to contribute

We just released a first stable version of _Dagger_. At the moment, the idea and execution of the library comes from a TODO



### Local development

We use Poetry to manage the dependencies of this library. In the codebase, you will find a `Makefile` with some useful commands to run and test your contributions. Namely:

- `make install` - Install the project's dependencies
- `make test` - Run tests and report test coverage. It will fail if coverage is too low.
- `make ci` - Run all the quality checks we run for each commit/PR. This includes type hint checking, linting, formatting and documentation.
- `make build` - Build the project's WHEEL
- `make docker-build` - Package the project in a Docker image
- `make docker-run-example-name` - Run any example DAG of those defined in "dagger/examples" (for instance, `make run-hello-world`)
- `make set-up-argo` - Create a k3d cluster for the project and installs Argo 3.0 in it. It also sets up a k3d image registry so that you can run the examples remotely.
- `make docker-push-local` - Build and push the project's Docker image to the local k3d registry.
- `make tear-down-argo` - Destroy the k3d cluster and registry where we deployed Argo.
