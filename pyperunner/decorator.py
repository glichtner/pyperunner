import inspect
from functools import wraps
from typing import Any, Callable

from pyperunner.task import Task


def task(name: str, receives_input: bool = True) -> Callable:
    """
    Decorator for turning a function into a Task to be used by pyperunner.

    This decorator can be used on any function, however, please note two important points:

    - You must specify a name for the task as the first parameter of this decorator
    - The function must either accept a keyword argument `data` or the task decorator must be
    supplied `receives_input=False

    Examples:

      .. code-block :: python

        # Create a task named "SimpleTask" - note that the function accepts the `data` keyword parameter
        @task('SimpleTaskWithInput')
        def simpletask_with_input(data):
            return 'simple-task'

        # If the function should not accept data (i.e. be a starting task of the pipeline), the `receives_input=False`
        # parameter must be supplied to the task() decorator.
        @task('SimpleTaskWithoutInput', receives_input=False)
        def simpletask_without_input():
            return 'simple-task'

        # The following definition will raise an AttributeError
        @task('SimpleTaskError')
        def simpletask_without_input_error():
            return 'simple-task'
        # raises AttributeError: To receive input data, the function must accept the named parameter "data"

    Args:
        name: Task name
        receives_input: True if the task should receive input (from a previous task) - then it must accept the keyword
          parameter `data`

    Returns: :py:class:`~pyperunner.Task` class implementing the function

    """

    def decorator(func: Callable) -> Callable:
        if receives_input:
            if "data" not in inspect.getfullargspec(func).args:
                raise AttributeError(
                    'To receive input data, the function must accept the named parameter "data"'
                )

        def wrapper(self: Task, data: Any) -> Task.TaskResult:
            return self.run_wrapper(
                func, data, static=True, receives_input=receives_input
            )

        # mark the wrapper as decorated to check if this decorator has been used on a Task.run function
        wrapper.__decorated__ = ["run"]  # type: ignore

        return type(
            name, (Task,), {"run": wrapper, "_run_signature": inspect.signature(func)}
        )

    if inspect.isfunction(name):
        raise TypeError(
            "Invalid decorator use - did you use @task instead of @task(name)?"
        )

    return decorator


def run(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(self: Task, data: Any) -> Task.TaskResult:
        return self.run_wrapper(func, data)

    # mark the wrapper as decorated to check if this decorator has been used on a Task.run function
    wrapper.__decorated__ = ["run"]  # type: ignore

    return wrapper
