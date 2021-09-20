from typing import Any

import yaml

from dagger import DeserializationError, SerializationError


class AsYAML:
    """Serializer implementation that uses YAML to marshal/unmarshal Python data structures."""

    extension = "yaml"

    def serialize(self, value: Any) -> bytes:
        """Serialize a value into a YAML object, encoded into binary format using utf-8."""
        try:
            return yaml.dump(value, Dumper=yaml.SafeDumper).encode("utf-8")
        except yaml.YAMLError as e:
            raise SerializationError(e)

    def deserialize(self, serialized_value: bytes) -> Any:
        """Deserialize a utf-8-encoded yaml object into the value it represents."""
        try:
            return yaml.load(serialized_value, Loader=yaml.SafeLoader)
        except yaml.YAMLError as e:
            raise DeserializationError(e)
