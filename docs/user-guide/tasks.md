# Tasks

Tasks are the basic building blocks of _Dagger_.

They __wrap a Python function__ that __will be executed as a step of a DAG__.

They also define:

- Where the inputs of the function are coming from (e.g. from a DAG parameter, from the output of another node...).
- What outputs does the task produce.
- How to retrieve the task's outputs from the return value of the function.


## 💡 Example

=== "Imperative DSL"

    ```python
    --8<-- "docs/code_snippets/task/imperative.py"
    ```

=== "Declarative Data Structures"

    ```python
    --8<-- "docs/code_snippets/task/declarative.py"
    ```



## ➡️ Inputs

A task can have multiple inputs.

The names of the inputs must correspond to those of the function's arguments.

Inputs can come:

* `#!python FromParam(name: str)`. This indicates the input comes from a parameter named `name`, passed to the task's parent (a DAG).
* `#!python FromNodeOutput(node: str, output: str)`. This indicates the input comes from an output named `output`, which comes from another node named `node`. The current task and the node must be siblings in the same DAG.


## ⬅️ Outputs

A task can have multiple outputs.

The specific type of output indicates how it should be retrieved from the return value of the function:

* `#!python FromReturnValue()` will expose the returned value as it is. That is, given a function `#!python lambda: {"a": 1, "b": 2}"`, it will return `#!python {"a": 1, "b": 2}`.
* `#!python FromKey(name: str)`. If the function returns a mapping, this output exposes the value of one of its keys. That is, given the function `#!python lambda: {"a": 1, "b": 2}"`, `#!python FromKey("a")` will return `1`.
* `#!python FromProperty(name: str)`. If the function returns an object, this output exposes the value of one of its properties. That is, given the function `#!python lambda: complex(2, 3)`, `#!python FromProperty("imag")` will return `3`.



## ⛔ Limitations

Tasks are validated against the following rules:

- Input and output names must be 1-64 characters long, begin by a letter or number, and only contain letters, numbers, underscores and hyphens.
- The names of the inputs must match the names of the function's arguments.
- The function should have as many inputs as the function has arguments. Optional arguments in the function still need to be supplied as inputs to the task.
- Tasks can only be partitioned by inputs that come from partitioned outputs. Therefore, they may not be partitioned by an input `FromParam()`.



## 📗 API Reference

Check the [API Reference](../api/task.md) for more details about the specific options supported by a Task.


## 🧠 Learn more about...

- How to connect multiple tasks together in a [Directed Acyclic Graph (DAG)](dags.md).
- How to use [different serializers](serializers/alternatives.md) for your inputs and outputs.


