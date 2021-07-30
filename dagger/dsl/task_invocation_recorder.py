"""Records task invocations during the definition of a DAG via the imperative DSL."""

import inspect
import uuid
from typing import Callable

import dagger.input as input
from dagger.dsl.context import node_invocations
from dagger.dsl.errors import TaskInvokedWithMismatchedArguments
from dagger.dsl.node_invocations import NodeInvocation, TaskInvocation
from dagger.dsl.node_outputs import NodeOutputReference, NodeOutputUsage
from dagger.dsl.parameter_usage import ParameterUsage


class TaskInvocationRecorder:
    """
    Records task invocations during the definition of a DAG via the imperative DSL.

    When an instance of this class is invoked, it records the invocation of a task in the DSL context.
    """

    def __init__(self, func: Callable):
        self._func = func

    def __call__(self, *args, **kwargs) -> NodeOutputUsage:
        """
        Record the invocation of this task with a series of arguments.

        Returns
        -------
        A representation of the task's output.

        Raises
        ------
        TaskInvokedWithMismatchedArguments
            When the arguments the task is invoked with do not match the arguments declared in the function's signature.

        NotImplementedError
            When some of the input/output types supported by dagger are not yet supported by the imperative DSL.
        """
        sig = inspect.signature(self._func)

        try:
            bound_args = sig.bind(*args, **kwargs)
        except TypeError as e:
            # TODO: Test this
            raise TaskInvokedWithMismatchedArguments(
                f"You have invoked the task '{self._func.__name__}' with the following arguments: args={args} kwargs={kwargs}. However, the signature of the function is '{sig}'. The following error was raised as a result of this mismatch: {e}"
            )

        inputs = {}
        for input_name, input_usage in bound_args.arguments.items():
            if isinstance(input_usage, ParameterUsage):
                param_name = (
                    input_usage.name if input_usage.name != input_name else None
                )
                inputs[input_name] = input.FromParam(name=param_name)
            elif isinstance(input_usage, NodeOutputReference):
                inputs[input_name] = input.FromNodeOutput(
                    input_usage.invocation_id, input_usage.output_name
                )
            else:
                raise NotImplementedError(
                    f"Type of input {type(input_usage)} not considered in the DSL yet"
                )

        invocation_id = uuid.uuid4()
        task_invocation = TaskInvocation(
            func=self._func,
            inputs=inputs,
            output=NodeOutputUsage(invocation_id=invocation_id),
        )

        node_invocation = NodeInvocation(
            id=invocation_id,
            name=self._func.__name__.replace("_", "-"),
            invocation=task_invocation,
        )
        node_invocations.set(node_invocations.get([]) + [node_invocation])

        return task_invocation.output

    @property
    def func(self):
        """Return the function recorded by this class."""
        return self._func
