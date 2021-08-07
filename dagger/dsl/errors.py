"""Different types of errors that may occur as a result of parsing the DAG defined through an imperative DSL."""

POTENTIAL_BUG_MESSAGE = "If you are seeing this error, this is probably a bug in the library. Please check our GitHub repository to see whether the bug has already been reported/fixed. Otherwise, please create a ticket."


class NodeInvokedWithMismatchedArgumentsError(TypeError):
    """Occurs when a node is invoked during the definition of a DAG, but the arguments supplied to the node do not match the signature of the function associated with it."""

    pass
