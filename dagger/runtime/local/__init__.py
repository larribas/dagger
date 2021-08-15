"""Run DAGs or nodes in memory."""

from typing import Any, Mapping, Optional

from dagger.dag import DAG, Node
from dagger.runtime.local.dag import _invoke_dag
from dagger.runtime.local.task import _invoke_task
from dagger.task import Task


def invoke(
    node: Node,
    params: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, bytes]:
    """
    Invoke a node with a series of parameters.

    Parameters
    ----------
    node
        Node to execute

    params
        Inputs to the task, indexed by input/parameter name.


    Returns
    -------
    Serialized outputs of the task, indexed by output name.


    Raises
    ------
    ValueError
        When any required parameters are missing

    TypeError
        When any of the outputs cannot be obtained from the return value of the task's function

    SerializationError
        When some of the outputs cannot be serialized with the specified Serializer
    """
    if isinstance(node, DAG):
        return _invoke_dag(node, params=params)
    elif isinstance(node, Task):
        return _invoke_task(node, params=params)
    else:
        raise NotImplementedError(
            f"Whoops, we were not expecting a node of type '{type(node).__name__}'. This type of nodes are not supported by the local runtime at the moment. If you believe this may be a bug, please report it to our GitHub repository."
        )
