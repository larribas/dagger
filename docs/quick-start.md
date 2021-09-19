# Quick Start

To get a first idea of what _Dagger_ can do, let's install it, create our first DAG and run it locally.


## Installation

_Dagger_ is published to the Python Package Index (PyPI) under the name `py-dagger`. To install it, you can simply run:

```
pip install py-dagger
```


## _Dagger_ in Action

### Defining a DAG using the imperative DSL

The following piece of code demonstrates how to build a DAG that performs a map-reduce operation on a series of numbers:

```python
--8<-- "docs/code_snippets/quick_start.py"
```

Let's take it step by step. First, we use the `dagger.dsl.task` decorator to define different tasks. Tasks in _Dagger_ are just Python functions. In this case, we have 3 tasks:

- `generate_numbers()` returns a list of numbers. The list has a variable length, to show how we can do dynamic loops.
- `raise_number(n, exponent)` receives a number and an exponent, and returns `n^exponent`.
- `sum_numbers(numbers)` receives a list of numbers and returns the sum of all of them.

Next, we use the `dagger.dsl.DAG` decorator on another function that invokes all the previously defined tasks and connects their inputs/outputs.

The example uses a for loop and appends elements to a list to gather all the different results. But you can try replacing it with something more _Pythonic_:

```python linenums="27"
@dsl.DAG()
def map_reduce_pipeline(exponent):
    return sum_numbers([raise_number(n, exponent) for n in generate_numbers()])
```


### Transforming the DAG into a set of data structures

After defining how our DAG should behave using a function decorated by `dagger.dsl.DAG`, we will need to use `dagger.dsl.build` to transform it into a `dagger.DAG` data structure, like this:

```python
dag = dsl.build(map_reduce_pipeline)
```

The variable `dag` now contains our pipeline expressed as a collection of data structures. These data structures validate that our DAG has been built correctly, and allow us to run it using one of the available runtimes.


### Running our DAG locally

The final step will be to test our DAG locally using the `dagger.runtime.local`.

Just do:

```python
from dagger.runtime.local import invoke

result = invoke(dag, params={"seed": 1, "exponent": 2})
print(f"The final result was {result}")
```

And you should see the results of the DAG printed to your screen.


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

- The [User Guide](user-guide/introduction.md) for an in-depth explanation of all the different components available, extensibility points and design decisions.
- The [API Reference](api/init.md).
- The [Examples](https://github.com/larribas/dagger/tree/main/examples).
