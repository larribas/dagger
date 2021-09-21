# DAG Composition

In _Dagger_, you can invoke DAGs inside of other DAGs.

This feature allows you to design complex data pipelines and __keep each group of steps clean and testable__.


## 💡 Example

Let's say we are modelling an ETL pipeline where we want to fetch a raw dataset and apply some transformations to it.

The list of transformations we apply may be arbitrarily complex and we want to keep track of each transformation separately, so we wrap each of them in a separate task.

In order to make our DAGs simpler and easier to test, we have decided to decouple the steps that transform the dataset from the steps that deal with database access, so we have created 2 different DAGs and invoked one from the other.

As your DAGs evolve and become more sophisticated, DAG composition will become one of your go-to features of the library.


=== "Imperative DSL"

    ```python
    --8<-- "docs/code_snippets/dag_composition/imperative.py"
    ```

=== "Declarative Data Structures"

    ```python
    --8<-- "docs/code_snippets/dag_composition/declarative.py"
    ```




## 🧠 Learn more about...

- How to work with [partitioned outputs and nodes](partitioning.md).
