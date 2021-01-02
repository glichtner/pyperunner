import logging
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict
import os
import joblib
from functools import wraps
import json
import hashlib
import pydot
import yaml
import networkx as nx
import importlib

from .dag import Node, DAG


def get_exception_str(exc: BaseException):
    s = traceback.format_exception(etype=None, value=exc, tb=exc.__traceback__)
    return "".join(s)


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

    def __init__(self, tag, reload=False, **kwargs):
        super().__init__(self.__class__.__name__ + f"({tag})")
        self.tag = tag
        self.task_name = self.__class__.__name__
        self.params: Dict = kwargs
        self.data_path: str = None
        self.output: Any = None
        self.status: Task.Status = Task.Status.NOT_STARTED
        self.result: Task.TaskResult = None
        self.reload: bool = reload
        self.logger = logging.getLogger(self.name)

    def _single_node_hash(self):
        s = json.dumps(
            {
                "class": self.__class__.__name__,
                "name": self.name,
                "params": self.params,
            },
            sort_keys=True,
        )
        s = s.encode("utf-8")
        return hashlib.md5(s).hexdigest()

    def _hash(self):
        hash = [self._single_node_hash() + "_" + self.name]

        for parent in self._parents_generator():
            hash += parent._hash()

        return sorted(hash)

    def hash(self):
        return hashlib.md5("/".join(self._hash()).encode("utf-8")).hexdigest()

    def description(self):
        return {
            "name": self.task_name,
            "module": self.__module__,
            "tag": self.tag,
            "hash": self.hash(),
            "params": self.params,
        }

    @abstractmethod
    def run(self):
        pass

    def run_wrapper(self, func, input):
        self.logger.info("Starting")

        if self.output_exists() and not self.reload:
            self.logger.info("Loading output from disk, skipping processing")
            output = self.load_output()
        else:
            try:
                self.store_params()
                output = func(self, data=input, **self.params)
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

        self.result = Task.TaskResult(
            status=Task.Status.SUCCESS, output=output, exception=None, traceback=None,
        )

        self.logger.info(f"Finished: {self.result.status}")

        return self.result

    def _parents_generator(self):
        for p in self.parents:
            if isinstance(p, Task):
                yield p

    def set_data_path(self, path: str):
        self.data_path = path

    def set_output(self, output: Any):
        self.output = output

    def set_status(self, status: "Task.Status"):
        self.status = status

    def set_reload(self, reload: bool):
        self.reload = reload

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

    def _build_caller_dict(self):
        params = {
            "task": self.description(),
            "parents": {
                parent.name: parent.description()
                for parent in self._parents_generator()
            },
        }

        return params

    def store_params(self):
        filename = self.output_filename("params.yaml")

        params = self._build_caller_dict()

        with open(filename, "w") as f:
            yaml.dump(params, f, default_flow_style=False)

    def load_output(self):
        filename = self.output_filename()
        return joblib.load(filename)

    def __str__(self):
        return f"{self.task_name}({self.tag})#{self.hash()}#{hash(self)}"


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

    def to_dict(self):
        g = self.create_graph()
        tasks = {}
        node: Task
        for node in g.nodes:
            desc = node.description()
            desc["parents"] = [p.name for p in g.predecessors(node)]
            tasks[node.name] = desc

        pipeline_dict = {"pipeline": {"name": self.name,}, "tasks": tasks}

        return pipeline_dict

    @staticmethod
    def __instantiate_task(class_name: str, module_name: str, tag: str, params: Dict):
        def get_class(class_name, module_name=None):

            if module_name is None:
                module_name = "__main__"

            module = importlib.import_module(module_name)

            if not hasattr(module, class_name):
                raise NameError(
                    f"Class '{class_name}' not found in module '{module_name}"
                )
            class_ = getattr(module, class_name)

            return class_

        c = get_class(class_name, module_name)

        assert issubclass(c, Task), f"Class '{class_name}' must be subclass of Task"

        task = c(tag, **params)

        return task

    @staticmethod
    def __instantiate_tasks(tasks_dict):
        tasks = {}
        for task_name, desc in tasks_dict.items():
            task = Pipeline.__instantiate_task(
                class_name=desc["name"],
                module_name=desc.get("module", None),
                tag=desc["tag"],
                params=desc["params"],
            )
            tasks[task_name] = task
        return tasks

    def __compare_hashes(self, tasks_dict):
        G = self.create_graph()
        for task in G.nodes:
            if "hash" not in tasks_dict[task.name]:
                continue
            if not tasks_dict[task.name]["hash"] == task.hash():
                raise ValueError(
                    "Cannot create pipeline from file: Hashes do not match"
                )

    def __connect_tasks(self, tasks, tasks_dict):
        G = nx.DiGraph()
        for task_name, desc in tasks_dict.items():
            G.add_node(task_name)
            for parent_task_name in desc["parents"]:
                G.add_edge(parent_task_name, task_name)

        for task_name in nx.topological_sort(G):
            if not tasks_dict[task_name]["parents"]:
                self(tasks[task_name])
            else:
                for parent_task_name in tasks_dict[task_name]["parents"]:
                    tasks[parent_task_name](tasks[task_name])

    @staticmethod
    def from_dict(pipeline_dict, compare_hashes=True):

        tasks_dict = pipeline_dict["tasks"]
        pipeline = Pipeline(**pipeline_dict["pipeline"])

        tasks = pipeline.__instantiate_tasks(tasks_dict)
        pipeline.__connect_tasks(tasks, tasks_dict)

        if compare_hashes:
            pipeline.__compare_hashes(tasks_dict)

        return pipeline

    def to_file(self, filename):
        with open(filename, "w") as f:
            yaml.dump(self.to_dict(), f)

    @staticmethod
    def from_file(filename, compare_hashes=True):
        with open(filename, "r") as f:
            pipeline_dict = yaml.load(f, Loader=yaml.FullLoader)
        return Pipeline.from_dict(pipeline_dict, compare_hashes=compare_hashes)
