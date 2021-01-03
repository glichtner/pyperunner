from typing import Dict, List, Type
import yaml
import networkx as nx
import importlib

from .task import Task
from .dag import Node, DAG


class Pipeline(DAG):
    colormap = {
        Task.Status.FAILURE: "red",
        Task.Status.SUCCESS: "green",
        Task.Status.NOT_STARTED: "white",
        Task.Status.RUNNING: "yellow",
        Task.Status.CANT_RUN: "gray",
        Task.Status.SKIPPED: "skyblue",
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

        pipeline_dict = {"pipeline": {"name": self.name}, "tasks": tasks}

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
            pipeline_dict = yaml.safe_load(f)
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
