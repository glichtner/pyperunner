from typing import Any, Optional, Dict, List, Union, Callable, Iterator
import hashlib
import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
import traceback

import joblib
import yaml

from .dag import Node


def get_exception_str(exc: BaseException) -> str:
    s = traceback.format_exception(etype=None, value=exc, tb=exc.__traceback__)
    return "".join(s)


class Task(Node, ABC):
    class Status(Enum):
        NOT_STARTED = 0
        FAILURE = -1
        SUCCESS = 1
        RUNNING = 2
        CANT_RUN = 3
        SKIPPED = 4

    @dataclass
    class TaskResult:

        status: "Task.Status"
        output: Any
        exception: Optional[Exception] = None
        traceback: str = ""

    def __init__(self, tag: str = "", reload: bool = False, **kwargs: Dict) -> None:
        super().__init__(self.__class__.__name__ + f"({tag})")
        self.tag = tag
        self.task_name = self.__class__.__name__
        self.params: Dict = kwargs
        self.data_path: str = ""
        self.output: Any = None
        self.status: Task.Status = Task.Status.NOT_STARTED
        self.result: Task.TaskResult = Task.TaskResult(
            status=Task.Status.NOT_STARTED, output=None
        )
        self.reload: bool = reload
        self.logger = logging.getLogger(self.name)

    def _single_node_hash(self) -> str:
        s = json.dumps(
            {
                "class": self.__class__.__name__,
                "name": self.name,
                "params": self.params,
            },
            sort_keys=True,
        )

        return hashlib.md5(s.encode("utf-8")).hexdigest()

    def _hash(self) -> List[str]:
        hash = [self._single_node_hash() + "_" + self.name]

        for parent in self._parents_generator():
            hash += parent._hash()

        return sorted(hash)

    def hash(self) -> str:
        return hashlib.md5("/".join(self._hash()).encode("utf-8")).hexdigest()

    def description(self) -> Dict[str, Union[str, Dict, List]]:
        return {
            "name": self.task_name,
            "module": self.__module__,
            "tag": self.tag,
            "hash": self.hash(),
            "params": self.params,
        }

    def should_run(self) -> bool:
        return not self.output_exists() or self.reload

    @abstractmethod
    def run(self, *args: List, **kwargs: Dict) -> None:
        pass

    def run_wrapper(
        self,
        func: Callable,
        input: Any,
        static: bool = False,
        receives_input: bool = True,
    ) -> TaskResult:
        self.logger.info("Starting")

        if self.output_exists() and not self.reload:
            self.logger.info("Loading output from disk, skipping processing")
            output = self.load_output()
        else:
            try:
                self.store_params()
                args = []
                kwargs = self.params.copy()
                if not static:
                    args.append(self)
                if receives_input:
                    kwargs["data"] = input

                output = func(*args, **kwargs)

                self.store_output(output)
            except Exception as e:
                self.logger.error(str(e))
                self.result = Task.TaskResult(
                    status=Task.Status.FAILURE,
                    output=None,
                    exception=e,
                    traceback=get_exception_str(e),
                )

                raise e

        self.result = Task.TaskResult(status=Task.Status.SUCCESS, output=output)

        self.logger.info(f"Finished: {self.result.status}")

        return self.result

    def _parents_generator(self) -> Iterator["Task"]:
        for p in self.parents:
            if isinstance(p, Task):
                yield p

    def set_data_path(self, path: str) -> None:
        self.data_path = path

    def set_output(self, output: Any) -> None:
        self.output = output

    def set_status(self, status: "Task.Status") -> None:
        self.status = status

    def set_reload(self, reload: bool) -> None:
        self.reload = reload

    def output_filename(self, filename: str = "result.dump.gz") -> str:
        path = os.path.realpath(os.path.join(self.data_path, self.name, self.hash()))

        if not os.path.exists(path):
            os.makedirs(path)

        return os.path.join(path, filename)

    def output_exists(self) -> bool:
        return os.path.exists(self.output_filename())

    def store_output(self, output: Any) -> None:
        filename = self.output_filename()
        joblib.dump(output, filename)

    def _build_caller_dict(self) -> Dict:
        params = {
            "task": self.description(),
            "parents": {
                parent.name: parent.description()
                for parent in self._parents_generator()
            },
        }

        return params

    def store_params(self) -> None:
        filename = self.output_filename("params.yaml")

        params = self._build_caller_dict()

        with open(filename, "w") as f:
            yaml.dump(params, f, default_flow_style=False)

    def load_output(self) -> Any:
        filename = self.output_filename()
        return joblib.load(filename)

    def __str__(self) -> str:
        return f"{self.task_name}({self.tag})#{self.hash()}#{hash(self)}"
