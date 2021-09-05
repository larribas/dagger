"""Define sophisticated workflows/pipelines as Directed Acyclic Graphs (DAGs) and execute them with different runtimes, either locally or remotely."""
import logging
import os

import dagger.dsl as dsl  # noqa
from dagger.dag import DAG  # noqa
from dagger.task import Task  # noqa

# We use dynamic versioning, coming from Git tags. 0.0.0 is a placeholder.
__version__ = "0.0.0"


LOG_FORMAT = "%(asctime)s %(levelname)s:%(name)s:%(message)s"
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format=LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
)
