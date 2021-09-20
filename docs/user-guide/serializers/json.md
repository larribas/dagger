# AsJSON

`AsJSON` serializes values to and from [JSON](https://www.json.org/json-en.html) using Python's standard [`json` library](https://docs.python.org/3/library/json.html).

__This is the default serialization mechanism for _Dagger_. If you don't override it, _Dagger_ will try to use JSON to serialize the output of each task.__


## Limitations: Serialization needs to be a symmetrical process

There are some types of objects in Python (such as named tuples) where the serialization and deserialization processes are not symmetrical. For instance:

```python
import json

original_t = MyNamedTuple(x=1, y=2)
serialized_t = json.dumps(original_t)
deserialized_t = json.loads(serialized_t)

assert original_t != deserialized_t
```

Python relies on the implementation of custom `JSONEncoder`s and `JSONDecoder`s to achieve symmetry.

If you want to serialize custom objects as JSON, we recommend you implement [your own serializer](write-your-own.md).


## API Reference

Check the [API Reference](../../api/serializer.md#asjson) for more details about this serializer.
