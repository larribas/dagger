# Parallelization: Output and node partitioning

Tasks in _Dagger_ can return partitioned outputs.

Partitioned outputs are just __outputs that can be split into several chunks__, and joined back together later on. The value of a partitioned output is expected to be an [Iterable](https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable).

__Nodes may also be partitioned based on one of their inputs__, provided that such an input comes from a partitioned output.

When a node is partitioned, __each of the partitions may be executed in parallel__. The outputs of a partitioned node are also partitioned.


## 💡 Example

Let's take the example from the section about [dag composition](dag-composition.md) and extend it a bit.

In that scenario, we were first retrieving a dataset from a source (e.g. a data lake), and then invoking a different DAG to apply multiple transformations to that dataset.

Now, we will assume the dataset we fetch can be arbitrarily large, so we will partition it into chunks according to some logic and invoke the set of transformations separately for each chunk.


=== "Imperative DSL"

    ```python
    --8<-- "docs/code_snippets/partitioning/imperative.py"
    ```

=== "Declarative Data Structures"

    ```python
    --8<-- "docs/code_snippets/partitioning/declarative.py"
    ```


## ⚙️ Performance and memory

One of the main tenets of _Dagger_ is that it should support arbitrarily large data pipelines without a big impact on its memory footprint.

In particular, the following design- and implementation-level decisions help keeping memory usage constant:

<!-- - Serializers work with buffered I/O types, allowing you to serialize and deserialize values backed by a local or remote file system. -->
- When working with partitioned outputs, __each partition is loaded lazily into memory__.

<!-- This means you should be able to use native Python types such as [Dask's DataFrames](https://docs.dask.org/en/latest/dataframe.html) to process large datasets and pass them between nodes without requiring the machine's memory to scale linearly. -->


## ⛔ Limitations

If not managed properly, combining partitioned outputs and nodes may lead to a lot of complexity (resulting in non-intuitive behavior for you, the library's users, and making the implementation of runtimes quite hard for anyone who wants to maintain or extend its behavior by [creating new runtimes](runtimes/write-your-own.md)).

When designing _Dagger_, __we made a conscious decision to limit the way in which partitioning can be used__. Our goal is to provide parallelization and map-reduce patterns that are easy to use and reason about, and rely on DAG composition and application code to combine the existing patterns into more sophisticated scenarios as elegantly as possible.

Here are some of the limitations of node and output partitioning:

* Nodes may only be partitioned by one of their inputs.
* Nodes may only be partitioned by an input that comes from a partitioned output.
* Nodes may NOT be partitioned by an input that comes from a parameter.
* DAGs may NOT return outputs that come directly from a partitioned node.

All these limitations can be overcome by the use of DAG composition and extra application code. __The section on [map-reduce patterns](map-reduce.md) provides useful solutions to all these limitations__.

!!! note
    Whenever you try to use a partitioning pattern that is not allowed, _Dagger_ will try to provide a useful error message explaining the root of the problem and pointing you to the documentation.


## 🧠 Learn more about...

- How to implement [map-reduce](map-reduce.md) patterns (also known as fan-out-fan-in, or scatter-gather patterns).
- How to run your DAGs with the different [runtimes](runtimes/alternatives.md).
