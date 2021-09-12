# DAGs

Tasks are the basic building blocks of _Dagger_.

They wrap a Python function that will be executed as a step of a DAG.

They also define:

- Where the inputs of the function are coming from (e.g. from a DAG parameter, from the output of another node...).
- What outputs does the task produce.
- How to retrieve the task's outputs from the return value of the function.


## Example

=== "Declarative Data Structures"

    ```python
    from dagger import DAG, FromKey, FromNodeOutput, FromParam, FromReturnValue, Task, dsl


    def prepare_datasets(sample_size):
        # ...
        return {
            "training": ["..."],
            "test": ["..."],
        }


    def train_model(training_dataset):
        # ...
        return "model"


    def measure_model_performance(model, test_dataset):
        # ...
        return "report"


    dag = DAG(
        inputs={
            "sample_size": FromParam(),
        },
        outputs={
            "performance_report": FromNodeOutput(
                "measure-model-performance", "performance_report"
            ),
        },
        nodes={
            "prepare-datasets": Task(
                prepare_datasets,
                inputs={
                    "sample_size": FromParam("sample_size"),
                },
                outputs={
                    "training_dataset": FromKey("training"),
                    "test_dataset": FromKey("test"),
                },
            ),
            "train-model": Task(
                train_model,
                inputs={
                    "training_dataset": FromNodeOutput(
                        "prepare-datasets", "training_dataset"
                    ),
                },
                outputs={
                    "model": FromReturnValue(),
                },
            ),
            "measure-model-performance": Task(
                measure_model_performance,
                inputs={
                    "model": FromNodeOutput("train-model", "model"),
                    "test_dataset": FromNodeOutput("prepare-datasets", "test_dataset"),
                },
                outputs={
                    "performance_report": FromReturnValue(),
                },
            ),
        },
    )
    ```

=== "Imperative DSL"

    ```python
    from dagger import dsl


    @dsl.task()
    def prepare_datasets(sample_size):
        # ...
        return {
            "training": ["..."],
            "test": ["..."],
        }


    @dsl.task()
    def train_model(training_dataset):
        # ...
        return "model"


    @dsl.task()
    def measure_model_performance(model, test_dataset):
        # ...
        return "report"


    @dsl.DAG()
    def dag(sample_size):
        datasets = prepare_datasets(sample_size)
        model = train_model(datasets["training"])
        return measure_model_performance(model, datasets["test"])
    ```


## Inputs

A DAG can have multiple inputs.

Inputs can come:

* `FromParam(name: str)`. This indicates the input comes from a paremeter named `name`, supplied when the DAG is executed.
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
- If a node input comes `FromNodeOutput(node: str, output: str)`, then the DAG must contain a node named `node` which exposes an output named `output`.
- DAGs may not contain cyclic dependencies. A cyclic dependency occurs whenever there is a chain of nodes `{n(1), n(2), ..., n(last)}` so that `n(i+1)` has an input that depends on an output of `n(i)`, but `n(1)` also contains an input that depends on the output of `n(last)`.



## API Reference

Check the [API Reference](../../api/dag.md) for more details about the specific options supported by the DAG class.


## Learn more about...

- How to [compose DAGs](dag-composition.md) (that is, nest DAGs inside of other DAGs).
- How to use [different serializers](../serializers/alternatives.md) for your inputs and outputs.
- How to work with [partitioned outputs and nodes](partitioning.md).
