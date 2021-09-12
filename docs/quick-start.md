# Quick Start

To get a first idea of what _Dagger_ can do, let's install it, create our first DAG and run it locally.


## Installation

_Dagger_ is published to the Python Package Index (PyPI) under the name `py-dagger`. To install it, you can simply run:

```
pip install py-dagger
```


## _Dagger_ in Action

The following piece of code demonstrates how to build a DAG that performs a map-reduce operation on a series of numbers:

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

Let's take it step by step. First, we use the `dagger.dsl.task` decorator to define different tasks. Tasks in _Dagger_ are just Python functions. In this case, we have 3 tasks:

- `generate_numbers()` returns a list of numbers. The list has a variable length, to show how we can do dynamic loops.
- `raise_number(n, exponent)` receives a number and an exponent, and returns `n^exponent`.
- `sum_numbers(numbers)` receives a list of numbers and returns the sum of all of them.

Next, we use the `dagger.dsl.DAG` decorator on another function that invokes all the previously defined tasks and connects their inputs/outputs.

The example uses a for loop and appends elements to a list to gather all the different results. But you can try replacing it with something more _Pythonic_:

```python linenums="21"
@dsl.DAG()
def map_reduce_pipeline(exponent):
    return sum_numbers([raise_number(n, exponent) for n in generate_numbers()])
```

Finally, we use `dagger.dsl.build` to transform that decorated function into a `dagger.dag.DAG` data structure, and we test it locally with `dagger.runtime.local.invoke`.


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
