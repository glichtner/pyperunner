import logging
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Callable, Union, Iterator, Type, Optional
import os
import joblib
import json
import hashlib
import pydot
import yaml
import networkx as nx
import importlib

from .dag import Node, DAG


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
        self.data_path: str
        self.output: Any = None
        self.status: Task.Status = Task.Status.NOT_STARTED
        self.result: Optional[Task.TaskResult] = None
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


class Pipeline(DAG):
    colormap = {
        Task.Status.FAILURE: "red",
        Task.Status.SUCCESS: "green",
        Task.Status.NOT_STARTED: "white",
        Task.Status.RUNNING: "yellow",
        Task.Status.CANT_RUN: "gray",
    }

    def __init__(self, name: str, tasks: List[Task] = None):
        super().__init__()
        self.name = name

        if tasks is not None:
            self.set_tasks(tasks)

    def set_tasks(self, tasks: List[Task]) -> None:
        for task in tasks:
            self.root.connect_child(task)

    def add(self, task: Task) -> None:
        self.root.connect_child(task)

    def _add_node(self, g: nx.DiGraph, node: Task) -> None:  # type: ignore[override]
        child: Task

        g.add_node(
            node, style="filled", fillcolor=self.colormap[node.status], label=node.name
        )

        for child in node.children:  # type: ignore
            if not g.has_node(child):
                self._add_node(g, child)
            g.add_edge(node, child)

    def to_dict(self) -> Dict:
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
    def __instantiate_task(
        class_name: str, module_name: str, tag: str, params: Dict
    ) -> Task:
        def get_class(class_name: str, module_name: str = None) -> Type:

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
    def __instantiate_tasks(tasks_dict: Dict) -> Dict:
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

    def __compare_hashes(self, tasks_dict: Dict) -> None:
        g = self.create_graph()
        for task in g.nodes:
            if "hash" not in tasks_dict[task.name]:
                continue
            if not tasks_dict[task.name]["hash"] == task.hash():
                raise ValueError(
                    "Cannot create pipeline from file: Hashes do not match"
                )

    def __connect_tasks(self, tasks: Dict, tasks_dict: Dict) -> None:

        g = nx.DiGraph()

        for task_name, desc in tasks_dict.items():
            g.add_node(task_name)
            for parent_task_name in desc["parents"]:
                g.add_edge(parent_task_name, task_name)

        for task_name in nx.topological_sort(g):
            if not tasks_dict[task_name]["parents"]:
                self(tasks[task_name])
            else:
                for parent_task_name in tasks_dict[task_name]["parents"]:
                    tasks[parent_task_name](tasks[task_name])

    @staticmethod
    def from_dict(pipeline_dict: Dict, compare_hashes: bool = True) -> "Pipeline":

        tasks_dict = pipeline_dict["tasks"]
        pipeline = Pipeline(**pipeline_dict["pipeline"])

        tasks = pipeline.__instantiate_tasks(tasks_dict)
        pipeline.__connect_tasks(tasks, tasks_dict)

        if compare_hashes:
            pipeline.__compare_hashes(tasks_dict)

        return pipeline

    def to_file(self, filename: str) -> None:
        with open(filename, "w") as f:
            yaml.dump(self.to_dict(), f)

    @staticmethod
    def from_file(filename: str, compare_hashes: bool = True) -> "Pipeline":
        with open(filename, "r") as f:
            pipeline_dict = yaml.load(f, Loader=yaml.FullLoader)
        return Pipeline.from_dict(pipeline_dict, compare_hashes=compare_hashes)


class Sequential(Pipeline):
    def set_tasks(self, tasks: List[Task]) -> None:
        prev_task: Node = self.root
        for task in tasks:
            prev_task.connect_child(task)
            prev_task = task

    def add(self, task: Task) -> None:
        cur: Node = self.root
        while cur.children:
            cur = cur.children[0]
        cur.connect_child(task)
