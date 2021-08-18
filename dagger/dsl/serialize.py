"""Annotate types to set specific serializers."""

from typing import Callable, Optional, get_type_hints

from dagger.serializer import DefaultSerializer, Serializer


class Serialize:
    """
    Indicate the serializer that should be used for the outputs of a specific node.

    This class is to be used together with typing Annotations, as such:

    ```
    @dsl.task
    def generate_single_number() -> Annotated[float, dsl.Serialize(AsPickle())]:
        return random.random()

    @dsl.task
    def generate_multiple_numbers() -> Annotated[
        Mapping[str, int],
        dsl.Serialize(json=AsJSON(indent=5), pickle=AsPickle()),
    ]:
        return { "json": 2, "pickle": 3 }
    ```

    In the previous example, task "generate_single_number" will return a single output,
    which will be serialized as a Pickle.

    Task "generate_multiple_numbers" will generate multiple outputs (potentially),
    each of them to be serialized using different serializers, if accessed on their own.

    Typing annotations are a feature of Python 3.9 that will allow us to use type hints
    that play nice with IDEs and code analysis tools, but allow us to give extra hints
    to the DSL parser.
    """

    def __init__(self, root: Serializer = DefaultSerializer, **kwargs: Serializer):
        self._root = root
        self._sub_outputs = kwargs

    @property
    def root(self) -> Serializer:
        """Return the serializer for the root output of the function."""
        return self._root

    def sub_output(self, output_name: str) -> Optional[Serializer]:
        """Return the serializer assigned to the output with the name provided, if any."""
        return self._sub_outputs.get(output_name, None)

    def __eq__(self, obj) -> bool:
        """Return true if the object is equivalent to the current instance."""
        return (
            isinstance(obj, Serialize)
            and self._root == obj._root
            and self._sub_outputs == obj._sub_outputs
        )

    def __repr__(self) -> str:
        """Return a human-readable representation of this class."""
        kv_serializers = [f"{k}={v}" for k, v in self._sub_outputs.items()]
        all_serializers = ", ".join([f"root={self._root}"] + kv_serializers)
        return f"Serialize({all_serializers})"


def find_serialize_annotation(func: Callable) -> Optional[Serialize]:
    """Get the serializers defined for the output."""
    hints = get_type_hints(func, include_extras=True)
    if "return" in hints and hasattr(hints["return"], "__metadata__"):
        serialize_annotations = [
            s for s in hints["return"].__metadata__ if isinstance(s, Serialize)
        ]
        if len(serialize_annotations) > 1:
            raise ValueError(
                f"The return value of '{func.__name__}' is annotated with multiple 'Serialize' annotations. This can lead to ambiguity about how to serialize its outputs, so we prefer to fail early and let you refactor your code. Please, remove this ambiguity."
            )

        if len(serialize_annotations) == 1:
            return serialize_annotations[0]

    return None
