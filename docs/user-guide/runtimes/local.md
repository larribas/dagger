# Local Runtime

The local runtime enables you to run any task or DAG locally. Just do:

=== "Imperative DSL"

    ```python
    --8<-- "docs/code_snippets/local_runtime/imperative.py"
    ```

=== "Declarative Data Structures"

    ```python
    --8<-- "docs/code_snippets/local_runtime/declarative.py"
    ```

## 🏠 Local Development and Testing

When you are developing your DAGs, you will want to test their behavior as frequently as possible. This practice helps you iterate more quickly.

The local runtime is great for local development and testing. On top of the validations that the data structures already perform, the local runtime serializes every output and connects all the nodes together, so __it is a great way to verify that all your nodes can communicate effectively__.

For data pipelines that deal with large amounts of data or take a long time to execute, we recommend you inject a parameter or environment variable named `#!python is_running_locally: bool` to your DAGs. Then, you can short-circuit some of the tasks based on the value of this parameter. For instance, a task that ingests several terabytes of data from a database may react to this parameter by ingesting less data, or even returning a fixture. This pattern will allow you to perform integration tests on your DAGs and still validate that they behave as expected.


## ⛔ Limitations

- The nodes in a DAG will run in the right order according to their dependencies (i.e. using their topological sorting). However, they will NOT run in parallel.


## 📗 API Reference

Check the [API Reference](../../api/runtime-local.md) for more details about this runtime.
