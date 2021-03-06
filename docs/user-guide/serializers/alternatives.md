# Serializers

When you run your DAGs with a runtime, the outputs produced by every task are serialized from a native Python type (such as a `str`, a `dict`, a `pd.DataFrame` or even a custom `class`) into a string of `bytes`.

Serializing outputs is necessary because two tasks may run on completely different physical machines, and _Dagger_ needs a consistent format to store them and transmit them over the network.

## 📦 Built-in Serializers

_Dagger_ comes with a few serializers built in. Namely:

* [`dagger.AsJSON`](json.md), which uses Python's [json library](https://docs.python.org/3/library/json.html).
* [`dagger.AsPickle`](pickle.md), which uses Python's [pickle library](https://docs.python.org/3/library/pickle.html)


## 🃏 Default Serializer: `AsJSON`

By default, outputs are serialized using JSON.

JSON works well for most basic types (`int`, `float`, `str`, `bool`, `dict`, `list` and `None` values). However, soon you will need to work with more complex types of values. For instance:

* You may be working with [Pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html), which you'd like to serialize as CSV or Parquet files.
* You may be working with contracts which you'd like to serialize using [Protocol Buffers](https://developers.google.com/protocol-buffers) or [Apache Avro](https://avro.apache.org/).

For those cases, ___Dagger_ allows you to change the serializers used for each value returned by a task, and implement your custom serializers__.


## 💡 Setting a specific serializer to use with an output

Say you have a task that returns an object which we want to serialize using the [Pickle protocol](https://docs.python.org/3/library/pickle.html). Here's how you can instruct _Dagger_ to do so:

=== "Imperative DSL"

    ```python
    --8<-- "docs/code_snippets/single_serializer/imperative.py"
    ```

=== "Declarative Data Structures"

    ```python
    --8<-- "docs/code_snippets/single_serializer/declarative.py"
    ```


### Different Serializers for Multiple Outputs

Now imagine the task actually returned multiple outputs:

- A boolean which can be serialized as JSON.
- A custom object which we want to serialize using Pickle.

Here's how you can instruct _Dagger_ to use a different serializer for each of the outputs:


=== "Imperative DSL"

    ```python
    --8<-- "docs/code_snippets/multiple_serializers/imperative.py"
    ```

=== "Declarative Data Structures"

    ```python
    --8<-- "docs/code_snippets/multiple_serializers/declarative.py"
    ```


## ⚙️ Performance and memory

One of the main tenets of _Dagger_ is that it should support arbitrarily large data pipelines without a big impact on its memory footprint.

To fulfill that promise, serializers work with I/O streams, allowing you to serialize and deserialize values backed by a local or remote file system.

This means you should be able to use native Python types such as [Dask's DataFrames](https://docs.dask.org/en/latest/dataframe.html) to process large datasets and pass them between nodes without requiring the machine's memory to scale linearly with the size of the datasets.


## 🛠️ Implementing your own serializer

To understand how to write your own serialization mechanism you can read [this guide](write-your-own.md).


