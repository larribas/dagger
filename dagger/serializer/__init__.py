"""Serialization strategies to pass inputs/outputs safely between tasks in a distributed environment."""

from dagger.serializer.errors import DeserializationError, SerializationError  # noqa
from dagger.serializer.json import JSON  # noqa
from dagger.serializer.protocol import Serializer  # noqa

# TODO: Make the default a Pickle serializer
DefaultSerializer = JSON()
