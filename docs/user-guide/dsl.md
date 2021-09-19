# Imperative DSL

TODO:
- Caution: This is metaprogramming, you can think of it as recording the intention of doing something but not actually doing it.
- The names of the outputs are always going to be "return_value", "property_x" and "key_x".
- Only outputs that are used will be saved. The rest will be omitted.
- Limitation: The @task and @DAG decorators will wrap the function and return an object of type `NodeInvocationRecorder`. This may be interpreted by some static code analyzers as something that contradicts the return value that you advertise on the function.
- You can still access the decorated function by doing `decorated_function.func == original_function`.
- Limitation: Defaults in tasks and DAGs are ignored (point to issue).
- Limitations: Pretty much everything not listed here is a limitation (or may not have the effect you intended to) when using the DSL: invoking a decorated function with the output of another function, a parameter or a literal value; iterating over the results of an invocation; adding all the results of a partitioned node in a list and passing that list to a fan-in node.
