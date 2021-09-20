# Write your own Serializer

Sometimes you may be dealing with objects that need to be serialized in a specific format that doesn't come out of the box with _Dagger_.

For example, you may want to serialize:

- A Pandas DataFrame as a CSV or a Parquet file.
- A Protocol Buffers contract.

In those cases, you can bring your custom serialization implementation to _Dagger_ and use it in your tasks.


## Serializer Protocol

To write a custom serializer, you need to create a class that implements the following protocol:


```python
--8<-- "dagger/serializer/protocol.py"
```


## Example: A YAML serializer

Say you want to serialize some of your outputs as YAML. Here's how a (slightly naive) implementation of a YAML serializer would look like:

```python
--8<-- "docs/code_snippets/yaml_serializer.py"
```
