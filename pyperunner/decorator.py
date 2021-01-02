import inspect
from functools import wraps
from typing import Any, Callable

from pyperunner import Task


def task(class_name: str, receives_input: bool = True) -> Callable:
    def decorator(func: Callable) -> Callable:
        if receives_input:
            if "data" not in inspect.getfullargspec(func).args:
                raise AttributeError(
                    'To receive input data, the function must accept the named parameter "data"'
                )

        def wrapper(self: Task, input: Any) -> Task.TaskResult:
            return self.run_wrapper(
                func, input, static=True, receives_input=receives_input
            )

        return type(class_name, (Task,), {"run": wrapper})

    return decorator


def run(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(self: Task, input: Any) -> Task.TaskResult:
        return self.run_wrapper(func, input)

    return wrapper
