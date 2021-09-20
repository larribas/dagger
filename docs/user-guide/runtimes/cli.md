# CLI Runtime

The CLI runtime is responsible for exposing a DAG through a Command-Line Interface.

This runtime is not meant to be used by humans, but by other runtimes, as a building block to run DAGs from container images or virtual machines.

The reason why it may be a bit tricky for humans to use is that it requires every input to be passed as a file whose contents are the serialized representation of that input's value.


## The Interface

Say you have a file `say_hello.py` that describes your DAG. You can use the CLI runtime to expose that DAG on the command line, like this:


=== "Imperative DSL"

    ```python
    --8<-- "docs/code_snippets/cli_runtime/imperative.py"
    ```

=== "Declarative Data Structures"

    ```python
    --8<-- "docs/code_snippets/cli_runtime/declarative.py"
    ```


If you invoke `python say_hello.py --help`, you will notice a message similar to this one:

```
usage: say_hello.py [-h] [--node-name NODE_NAME] [--output name location]
              [--input name location]

Run a DAG, either completely, or partially using the filters specified in the
arguments

optional arguments:
  -h, --help            show this help message and exit
  --node-name NODE_NAME
                        Select a specific node to run. It must be properly
                        namespaced with the name of all the parent DAGs.
  --output name location
                        Store a given output into the location specified.
                        Currently, we only support storing outputs in the
                        local filesystem
  --input name location
                        Retrieve a given input from the location specified.
                        Currently, we only support retrieving inputs from the
                        local filesystem
```


As you can see, you can do 3 things with the CLI:

- You can select a specific node for execution (try doing `pythong say_hello --node-name=say-hello`).
- You can pass any number of inputs. The location of each input needs to be a local file that contains the serialized value of the input.
- You can pass any number of outputs. The location of each output needs to be a local file where the serialized value of the output will be stored.


## API Reference

Check the [API Reference](../../api/runtime-cli.md) for more details about this runtime.
