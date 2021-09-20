"""Define sophisticated data pipelines as Directed Acyclic Graphs (DAGs) and execute them with different runtimes, either locally or remotely."""

import dagger.dsl as dsl  # noqa
from dagger.dag import DAG  # noqa
from dagger.input import FromNodeOutput, FromParam  # noqa
from dagger.output import FromKey, FromProperty, FromReturnValue  # noqa
from dagger.serializer import (  # noqa
    AsJSON,
    AsPickle,
    DeserializationError,
    SerializationError,
    Serializer,
)
from dagger.task import Task  # noqa

# This will be replaced at package publication time by the latest git tag
__version__ = "0.0.0"
