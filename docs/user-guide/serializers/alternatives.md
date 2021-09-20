# Alternatives

When you run your DAGs with a runtime, the outputs produced by every task are serialized from a native Python type (such as a `str`, a `dict`, a `pd.DataFrame` or even a custom `class`) into a string of `bytes`.

Serializing outputs is necessary because two tasks may run on completely different physical machines, and _Dagger_ needs a consistent format to store them and transmit them over the network.

## Built-in Serializers

_Dagger_ comes with a few serializers built in. Namely:

* [`dagger.AsJSON`](json.md), which uses Python's [json library](https://docs.python.org/3/library/json.html).
* [`dagger.AsPickle`](pickle.md), which uses Python's [pickle library](https://docs.python.org/3/library/pickle.html)


## Default Serializer: AsJSON

By default, outputs are serialized using `AsJSON`.

JSON works well for most basic types (`int`, `float`, `str`, `bool`, `dict`, `list` and `None` values). However, you will most likely need to work with other types of values. For instance:

* You may be working with `pd.DataFrame` values, which you'd like to serialize as CSVs or Parquet files.
* You may be working with contracts which you'd like to serialize using [Protocol Buffers](https://developers.google.com/protocol-buffers) or [Apache Avro](https://avro.apache.org/).

For those cases, _Dagger_ allows you to __set specific serializers__ for each value returned by a task, and __implement your custom serializer__.


## Setting a specific serializer to use with an output

Say you have a task that returns an object which we want to serialize using pickle. Here's how you can instruct _Dagger_ to do so:

=== "Imperative DSL"

    ```python
    --8<-- "docs/code_snippets/single_serializer/imperative.py"
    ```

=== "Declarative Data Structures"

    ```python
    --8<-- "docs/code_snippets/single_serializer/declarative.py"
    ```


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


## Implementing your own serializer

To understand how to write your own serialization mechanism you can read [this guide](write-your-own.md).


