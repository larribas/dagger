"""Different types of errors that may occur as a result of parsing the DAG defined through an imperative DSL."""


class TaskInvokedWithMismatchedArguments(TypeError):
    """Occurs when a task was invoked during the definition of a DAG, but the arguments supplied to the task did not match the signature of the function associated with the task."""

    pass
