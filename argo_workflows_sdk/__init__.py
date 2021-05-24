import logging
import os

from argo_workflows_sdk.dag import DAG, DAGOutput
from argo_workflows_sdk.node import Node

__version__ = "0.1.0"

LOG_FORMAT = "%(asctime)s %(levelname)s:%(name)s:%(message)s"
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format=LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
)

__all__ = [
    DAG,
    DAGOutput,
    Node,
]
