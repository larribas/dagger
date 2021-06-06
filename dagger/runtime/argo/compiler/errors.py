"""Specific errors that may occur as a result of compiling a DAG into Argo Workflows manifests."""


class IncompatibilityError(Exception):
    """Error that occurs when the Argo runtime is not compatible with some of the types of inputs, outputs or nodes in the DAG."""

    pass
