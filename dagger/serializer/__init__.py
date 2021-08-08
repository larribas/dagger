"""Serialization strategies to pass inputs/outputs safely between tasks in a distributed environment."""

from dagger.serializer.as_json import AsJSON  # noqa
from dagger.serializer.errors import DeserializationError, SerializationError  # noqa
from dagger.serializer.protocol import Serializer  # noqa

# TODO: Make the default a Pickle serializer
DefaultSerializer = AsJSON()
