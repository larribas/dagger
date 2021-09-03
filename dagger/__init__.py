"""Define sophisticated workflows/pipelines as Directed Acyclic Graphs (DAGs) and execute them with different runtimes, either locally or remotely."""
import logging
import os

from dagger.dag import DAG  # noqa
from dagger.task import Task  # noqa

__version__ = "0.1.0"


LOG_FORMAT = "%(asctime)s %(levelname)s:%(name)s:%(message)s"
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format=LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
)
