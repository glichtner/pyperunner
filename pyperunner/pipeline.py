from typing import Dict, List, Type, Optional
import yaml
import networkx as nx
import importlib

from pyperunner.dag import Node, DAG
from pyperunner.task import Task
from pyperunner.util import PipelineResult


class PipelineError(Exception):
    pass


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
        """
        A pipeline describes which Tasks should be executed and in what order.

        The pipeline is implemented as a directed, acyclic graph of :py:class:`Task` objects.

        Args:
            name: Name of the pipeline (supply any name that helps you later identifying what the pipeline was used for)
            tasks: Optional list of *primary* tasks, i.e. tasks at which the pipeline will start (there will be no
                connection between these tasks)
        """
        super().__init__()
        self.name = name
        self._results: Optional[PipelineResult] = None

        if tasks is not None:
            self.set_tasks(tasks)

    def set_tasks(self, tasks: List[Task]) -> None:
        """
        Sets the supplied tasks as *primary* tasks of the pipeline ("root tasks"), i.e. tasks at which the pipeline
        will start (there will be no connection between these tasks)

        Args:
            tasks: Tasks to be set as primary tasks (root tasks)

        Returns: None

        """
        for task in tasks:
            self.root.connect_child(task)

    def add(self, task: Task) -> None:
        """
        Add a single task as a *primary* task of the pipeline ("root task"), i.e. a task at which the pipeline starts.

        Args:
            task: Task to set as primary (root) task

        Returns: None

        """
        self.root.connect_child(task)

    def _add_node(self, g: nx.DiGraph, node: Task) -> None:  # type: ignore[override]
        """
        Recursively add a node and all of its children to a given graph.

        This function is used when creating the nx.DiGraph representation of the pipeline to add all the tasks to the
        graph. As additional information, a "fillcolor" attribute is set according to the task's status
        (see :py:attr:`~Pipeline.colormap`). This is used when creating a PNG image of the pipeline via
        :py:meth:`~DAG.plot_graph`.

        Args:
            g: Graph to add the node to
            node: Node to add

        Returns:

        """
        child: Task

        g.add_node(
            node, style="filled", fillcolor=self.colormap[node.status], label=node.name
        )

        for child in node.children:  # type: ignore
            if not g.has_node(child):
                self._add_node(g, child)
            g.add_edge(node, child)

    def to_dict(self) -> Dict:
        """
        Get a dictionary representation of the directed acyclic graph underlying the pipeline.

        Returns: Dictionary representation of the directed acyclic graph underlying the pipeline.

        """
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
        """
        Helper function for :py:meth:`Pipeline.from_dict` for instantiating :py:class:`Task`s from string class names

        Args:
            class_name: Class name of the task object to instantiate
            module_name: Module name of the class
            tag: Tag of the task object to instantiate
            params:  Parameter of the task object to instantiate

        Returns: Instantiated Task

        """

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
        """
        Helper function for :py:meth:`Pipeline.from_dict` for instantiating :py:class:`Task`s from string class names

        Args:
            tasks_dict: Dictionary with Task information from saved parameter.yaml file
                Requires the keys "name", "module", "tag" and "params"

        Returns: Collection of instantiated tasks

        """

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
        """
        Compare the hashs of a Tasks dictionary (from parameter.yaml file) and the instantiated tasks of the pipeline

        Used to ensure that tasks created using :py:meth:`Pipeline.from_dict` were created correctly.

        Args:
            tasks_dict: Tasks dictionary from saved parameter.yaml file

        Returns: None

        """
        g = self.create_graph()
        for task in g.nodes:
            if "hash" not in tasks_dict[task.name]:
                continue
            if not tasks_dict[task.name]["hash"] == task.hash():
                raise ValueError(
                    "Cannot create pipeline from file: Hashes do not match"
                )

    def __connect_tasks(self, tasks: Dict, tasks_dict: Dict) -> None:
        """
        Connect task objects according to a task dictionary from a saved parameter.yaml file

        Takes the list of instantiated tasks and the information of how tasks are connected to each other from the
        dictionary (usually read from parameter.yaml file) and connects the Tasks accordingly in the pipeline.

        Args:
            tasks: Collection of instantiated tasks
            tasks_dict: Task dictionary from saved parameter.yaml file

        Returns: None

        """

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
        """
        Create a pipeline from a dictionary (usually saved parameter.yaml file from previous pipeline run)

        Args:
            pipeline_dict: Dictionary from saved pipeline run (parameter.yaml)
            compare_hashes: Set True if the hashes stored in the pipeline_dict dictionary should be compared with the
                hashes of the task objects created by this function. Used to ensure that Tasks are created in accordance
                with the saved pipeline run.

        Returns: Pipeline with all tasks as defined by dictionary

        """

        tasks_dict = pipeline_dict["tasks"]
        pipeline = Pipeline(**pipeline_dict["pipeline"])

        tasks = pipeline.__instantiate_tasks(tasks_dict)
        pipeline.__connect_tasks(tasks, tasks_dict)

        if compare_hashes:
            pipeline.__compare_hashes(tasks_dict)

        return pipeline

    def to_file(self, filename: str) -> None:
        """
        Save a representation of the pipeline to file (yaml format)

        Args:
            filename: Filename to store the representation to

        Returns: None

        """
        with open(filename, "w") as f:
            yaml.dump(self.to_dict(), f)

    @staticmethod
    def from_file(filename: str, compare_hashes: bool = True) -> "Pipeline":
        """
        Create a pipeline from a stored file yaml file.

        Args:
            filename: Filename of the yaml file where the pipeline is stored
            compare_hashes: Set True if the hashes stored in the pipeline_dict dictionary should be compared with the
                hashes of the task objects created by this function. Used to ensure that Tasks are created in accordance
                with the saved pipeline run.
        Returns: Pipeline with all tasks as defined by the file

        """
        with open(filename, "r") as f:
            pipeline_dict = yaml.safe_load(f)
        return Pipeline.from_dict(pipeline_dict, compare_hashes=compare_hashes)

    def set_results(self, results: PipelineResult) -> None:
        """
        Stores results of a pipeline run

        Args:
            results: The pipeline results

        Returns: None

        """
        self._results = results

    def results(self) -> PipelineResult:
        if self._results is None:
            raise AttributeError("Results have not been set. Run the pipeline first!")
        return self._results


class Sequential(Pipeline):
    """
    A purely sequential pipeline with no bifurcations (linear pipeline).
    """

    def set_tasks(self, tasks: List[Task]) -> None:
        """
        Set the tasks of the current pipeline.

        Note: The tasks are connected in the same order as they are in the supplied list, so make sure these are ordered
        correctly.
        Note: The set_tasks() method of the Sequential pipeline behaves differently from the
        :py:meth:`Pipeline.set_tasks` method.

        Args:
            tasks: Ordered list of tasks, with first element (tasks[0]) being the first task to run (root task)

        Returns: None

        """
        prev_task: Node = self.root
        for task in tasks:
            prev_task.connect_child(task)
            prev_task = task

    def add(self, task: Task) -> None:
        """
        Add a task to the current end of the pipeline.

        Note: The add() method of the Sequential pipeline behaves differently from the
        :py:meth:`Pipeline.add` method.

        Args:
            task: Task to add as a successor of the current end of the pipeline.

        Returns: None

        """
        cur: Node = self.root
        while cur.children:
            cur = cur.children[0]
        cur.connect_child(task)
