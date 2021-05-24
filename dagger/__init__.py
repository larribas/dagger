import logging
import os

from dagger.dag import DAG, DAGOutput
from dagger.node import Node

__version__ = "0.1.0"


LOG_FORMAT = "%(asctime)s %(levelname)s:%(name)s:%(message)s"
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format=LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
)

__all__ = [
    DAG.__name__,
    DAGOutput.__name__,
    Node.__name__,
]
