import inspect
from functools import wraps
from typing import Any

from pyperunner import Task


def task(class_name, receives_input=False):
    def decorator(func):
        if receives_input:
            if not "data" in inspect.getfullargspec(func).args:
                raise AttributeError(
                    'To receive input data, the function must accept the named parameter "data"'
                )

        def wrapper(self: Task, input):
            return self.run_wrapper(
                func, input, static=True, receives_input=receives_input
            )

        return type(class_name, (Task,), {"run": wrapper})

    return decorator


def run(func):
    @wraps(func)
    def wrapper(self: Task, input: Any):
        return self.run_wrapper(func, input)

    return wrapper
