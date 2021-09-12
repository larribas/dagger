# Tasks

Tasks are the basic building blocks of _Dagger_.

They wrap a Python function that will be executed as a step of a DAG.

They also define:

- Where the inputs of the function are coming from (e.g. from a DAG parameter, from the output of another node...).
- What outputs does the task produce.
- How to retrieve the task's outputs from the return value of the function.


## Example

=== "Declarative Data Structures"

    ```python
    from dagger import Task, FromParam, FromReturnValue

    def hello(name):
      return f"Hello {name}!"

    task = Task(
      hello,
      inputs={
        "name": FromParam(),
      },
      outputs={
        "hello_message": FromReturnValue(),
      },
    )
    ```

=== "Imperative DSL"

    ```python
    from dagger import dsl

    @dsl.task()
    def hello(name):
        return f"Hello {name}!"
    ```


## Inputs

A task can have multiple inputs.

The names of the inputs must correspond to those of the function's arguments.

Inputs can come:

* `FromParam(name: str)`. This indicates the input comes from a paremeter named `name`, passed to the task's parent (a DAG).
* `FromNodeOutput(node: str, output: str)`. This indicates the input comes from an output named `output`, which comes from another node named `node`. The current task and the node must be siblings in the same DAG.


## Outputs

A task can have multiple outputs.

Output names must be 1-64 characters long, and only contain letters, numbers, underscores and hyphens.

The specific type of output indicates how it should be retrieved from the return value of the function:

* `FromReturnValue()` will expose the returned value as it is. That is, given a function `lambda: {"a": 1, "b": 2}"`, it will return `{"a": 1, "b": 2}`.
* `FromKey(name: str)`. If the function returns a mapping, this output exposes the value of one of its keys. That is, given the function `lambda: {"a": 1, "b": 2}"`, `FromKey("a") will return `1`.
* `FromProperty(name: str)`. If the function returns an object, this output exposes the value of one of its properties. That is, given the function `lambda: complex(2, 3)`, `FromProperty("imag")` will return `3`.



## Limitations

Tasks are validated against the following rules:

- Input and output names must be 1-64 characters long, begin by a letter or number, and only contain letters, numbers, underscores and hyphens.
- The names of the inputs must match the names of the function's arguments.
- The function should have as many inputs as the function has arguments. Optional arguments in the function still need to be supplied as inputs to the task.
- Tasks can only be partitioned by inputs that come from partitioned outputs. Therefore, they may not be partitioned by an input `FromParam()`.



## API Reference

Check the [API Reference](../../api/task.md) for more details about the specific options supported by a Task.


## Learn more about...

- How to connect multiple tasks together in a [Directed Acyclic Graph (DAG)](dags.md).
- How to use [different serializers](../serializers/alternatives.md) for your inputs and outputs.


