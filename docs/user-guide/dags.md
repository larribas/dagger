# DAGs

Directed Acyclic Graphs (DAGs) contain multiple nodes and describe how their nodes are connected to each other.

Nodes in a DAG may be either Tasks or other DAGs, allowing you to [compose DAGs](dag-composition.md).

Nodes are connected to each other through their inputs and outputs. If a node `x` has an input that depends on the output of another node `y`, _Dagger_ will register a dependency of `y -> x`, and runtimes will execute `x` before `y` according to their [topological ordering](https://en.wikipedia.org/wiki/Topological_sorting#:~:text=In%20computer%20science%2C%20a%20topological,before%20v%20in%20the%20ordering.&text=Topological%20sorting%20has%20many%20applications,such%20as%20feedback%20arc%20set.).

All dependencies in _Dagger_ are defined implicitly by the relationship between the nodes' inputs and outputs. It is not possible to declare a dependency explicitly. If two nodes do not depend on each other's outputs, _Dagger_ will assume they can be executed independently, potentially in parallel.

DAGs can themselves have inputs and outputs:

* The inputs of a DAG may come from a parameter, or from the output of a sibling node (if the DAG is nested inside of another DAG).
* The outputs of a DAG must come from the output of one of its nodes.



## Example

The following code snippet describes a simple DAG that mocks a simplified training pipeline for a Machine Learning model.

The DAG has 3 tasks:

* A first task prepares the training and test datasets for the model (note how the task has 2 outputs).
* A second task trains the model based on the training dataset.
* A third task measures the performance of the trained model against the test dataset.

=== "Imperative DSL"

    ```python
    --8<-- "docs/code_snippets/dag/imperative.py"
    ```

=== "Declarative Data Structures"

    ```python
    --8<-- "docs/code_snippets/dag/declarative.py"
    ```


## Inputs

A DAG can have multiple inputs.

Inputs can come:

* `FromParam(name: str)`. This indicates the input comes from a parameter named `name`, supplied when the DAG is executed.
* `FromNodeOutput(node: str, output: str)`. This indicates the input comes from an output named `output`, which comes from another node named `node`. The current DAG and the node must be siblings in the same parent DAG.


## Outputs

A DAG does not produce outputs of its own. Instead, it just exposes the output(s) of some of its nodes. Thus, outputs are always of type: `FromNodeOutput(node: str, output: str)`, where `node` points to the name of one of the DAG's nodes, and `output` to the name of the output to expose from that node.

Output names must be 1-64 characters long, and only contain letters, numbers, underscores and hyphens.


## Limitations

DAGs are validated against the following rules:

- DAGs should have at least one node. There would be no point in having an empty DAG.
- Node names must be 1-64 characters long, begin by a letter or number, and only contain letters, numbers and hyphens.
- Input and output names have the same constraints as node names, but they can also contain underscores.
- If a node input comes `FromParam(name: str)`, then the DAG must have an input named `name`.
- If a node input comes `FromNodeOutput(node: str, output: str)`, then the DAG must contain a node named `node` which exposes an output named `output`.
- DAGs may not contain cyclic dependencies. A cyclic dependency occurs whenever there is a chain of nodes `{n(1), n(2), ..., n(last)}` so that `n(i+1)` has an input that depends on an output of `n(i)`, but `n(1)` also contains an input that depends on the output of `n(last)`.



## API Reference

Check the [API Reference](../api/dag.md) for more details about the specific options supported by the DAG class.


## Learn more about...

- How to [compose DAGs](dag-composition.md) (that is, nest DAGs inside of other DAGs).
- How to work with [partitioned outputs and nodes](partitioning.md).
- How to run your DAGs with the different [runtimes](runtimes/alternatives.md).
