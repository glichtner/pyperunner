import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict
import os
import joblib
import time
from functools import wraps
import json
import hashlib
import pydot
import yaml

from .dag import Node, DAG


class Task(Node, ABC):
    class Status(Enum):
        NOT_STARTED = 0
        FAILURE = -1
        SUCCESS = 1
        RUNNING = 2
        CANT_RUN = 3

    @dataclass
    class TaskResult:

        status: "Task.Status"
        output: Any
        exception: Exception
        traceback: str

    def __init__(self, name, wait=1, reload=False, **kwargs):
        super().__init__(self.__class__.__name__ + f"({name})")
        self.params: Dict = kwargs
        self.wait: int = wait
        self.data_path: str = None
        self.output: Any = None
        self.status: Task.Status = Task.Status.NOT_STARTED
        self.result: Task.TaskResult = None
        self.reload: bool = reload

    def _single_node_hash(self):
        s = json.dumps(
            {"class": self.__class__.__name__, "name": self.name, "params": self.params}
        )
        s = s.encode("utf-8")
        return hashlib.md5(s).hexdigest()

    def _hash(self):
        hash = [self._single_node_hash() + "_" + self.name]

        for parent in self.parents:
            hash += parent._hash()

        return hash

    def hash(self):
        return hashlib.md5("/".join(self._hash()).encode("utf-8")).hexdigest()

    @abstractmethod
    def run(self):
        pass

    def run_wrapper(self, func, input):
        print(self.name, "start")

        if self.output_exists() and not self.reload:
            print(self.name, "skipped")
            output = self.load_output()
        else:
            time.sleep(self.wait)
            try:
                self.store_params()
                output = func(self, data=input, **self.params)
                self.store_output(output)
            except Exception as e:
                self.result = Task.TaskResult(
                    status=Task.Status.FAILURE,
                    output=None,
                    exception=e,
                    traceback=traceback.format_exc(),
                )
                raise e

        self.result = Task.TaskResult(
            status=Task.Status.SUCCESS, output=output, exception=None, traceback=None,
        )

        # print(self.name, input, result.output, result.status)
        print(self.name, "finish", self.result.status)

        return self.result

    def set_data_path(self, path: str):
        self.data_path = path

    def set_output(self, output: Any):
        self.output = output

    def set_status(self, status: "Task.Status"):
        self.status = status

    def output_filename(self, filename="result.dump.gz"):
        path = os.path.realpath(os.path.join(self.data_path, self.name, self.hash()))
        if not os.path.exists(path):
            os.makedirs(path)
        return os.path.join(path, filename)

    def output_exists(self):
        return os.path.exists(self.output_filename())

    def store_output(self, output):
        filename = self.output_filename()
        joblib.dump(output, filename)

    def store_params(self):
        filename = self.output_filename("params.yaml")
        with open(filename, "w") as f:
            yaml.dump(self.params, f, default_flow_style=False)

    def load_output(self):
        filename = self.output_filename()
        return joblib.load(filename)

    def __str__(self):
        return self.name + "#" + self.hash() + "#" + str(hash(self))


def run(func):
    @wraps(func)
    def wrapper(self: Task, input: Any):
        return self.run_wrapper(func, input)

    return wrapper


class Pipeline(DAG):
    colormap = {
        Task.Status.FAILURE: "red",
        Task.Status.SUCCESS: "green",
        Task.Status.NOT_STARTED: "white",
        Task.Status.RUNNING: "yellow",
        Task.Status.CANT_RUN: "gray",
    }

    def __init__(self, name):
        super().__init__()
        self.name = name

    def run(self):
        pass

    def _add_node(self, G, node: Task):
        child: Task

        G.add_node(
            node, style="filled", fillcolor=self.colormap[node.status], label=node.name
        )

        for child in node.children:
            self._add_node(G, child)
            G.add_edge(node, child)
