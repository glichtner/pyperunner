import traceback
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from queue import Empty
from typing import List, Set, Any, Dict

import os

import joblib
import networkx as nx
import multiprocessing
import time
import random
from functools import wraps
import json
import hashlib
import pydot
import yaml

# inspired by https://github.com/ecdavis/multiprocessing_dag


class Node:
    def __init__(self, name):
        self.name = name
        self.children = []
        self.parents = []

    def add_child(self, other):
        self.children.append(other)

    def add_parent(self, other):
        self.parents.append(other)

    def __call__(self, x):
        self.add_child(x)
        x.add_parent(self)

        return x

    def __str__(self):
        return self.name


class DAG:
    def __init__(self):
        self.root = Root()

    def __call__(self, x):
        self.root(x)
        return x

    def _add_node(self, G: nx.DiGraph, node: Node):
        G.add_node(node)

        for child in node.children:
            self._add_node(G, child)
            G.add_edge(node, child)

    def create_graph(self):
        G = nx.DiGraph()

        for child in self.root.children:
            self._add_node(G, child)
        return G

    def plot_graph(self):
        G = self.create_graph()
        gp = nx.drawing.nx_pydot.to_pydot(G)
        gp.set_simplify(True)
        img = gp.create_png()
        return img


class Root(Node):
    def __init__(self):
        super().__init__("root")

    def _hash(self):
        return [hashlib.md5("root".encode("utf-8")).hexdigest()]


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


class LoadDataTask(Task):
    @run
    def run(self, data, case_type):
        return f"load-data-task({data})"


class ProcessDataTask(Task):
    @run
    def run(self, data, param, other_param=False):

        if input == "load-data-task(covid)":
            raise Exception("test")

        return f"process-data-task({input})"


class EvaluateDataTask(Task):
    @run
    def run(self, data):
        return f"evaluate-data-task({input})"


class Process(multiprocessing.Process):
    def __init__(self, task: Task, queue: multiprocessing.Queue):
        super().__init__()  # name=f"task-{task.uri()}")  # TODO don't use uri

        self.task = task
        self.queue = queue
        # self.logger = logging.getLogger(task.uri())  # TODO uri?

    def _get_input(self):
        try:
            input = self.queue.get_nowait()
        except Empty:
            input = None

        return input

    def run(self):
        input = self._get_input()
        try:
            self.task.run(input)
        except Exception as e:
            raise e
        finally:
            self.queue.put(self.task.result)
            self.queue.close()


class Runner:
    def __init__(self, data_path, log_path, process_limit=multiprocessing.cpu_count()):
        self.tasks_finished: Set[Task] = set()
        self.tasks_error: Set[Task] = set()
        self.tasks_queue: List[Task] = []
        self.proc_running: List[Process] = []

        self.process_limit: bool = process_limit
        self.pipeline: Pipeline = None
        self.G: nx.DiGraph = None

        if not os.path.exists(data_path):
            os.makedirs(data_path)
        self.data_path: str = data_path

        if not os.path.exists(log_path):
            os.makedirs(log_path)
        self.log_path: str = log_path

    def dequeue(self):
        for task in self.tasks_queue:
            predecessor_tasks = set(self.G.predecessors(task))
            if predecessor_tasks & set(self.tasks_error):
                # this task cannot be run anymore as at least on predecessor failed
                task.set_status(Task.Status.CANT_RUN)
                self.tasks_queue.remove(task)
            if predecessor_tasks.issubset(self.tasks_finished):
                self.tasks_queue.remove(task)
                return task

    def get_predecessor_outputs(self, task):
        return {str(pt): pt.output for pt in self.G.predecessors(task)}

    def start_task(self, task):
        queue = multiprocessing.Queue()
        queue.put(self.get_predecessor_outputs(task))
        task.set_data_path(self.data_path)
        proc = Process(task, queue)
        proc.start()
        task.set_status(Task.Status.RUNNING)
        return proc

    def finish_tasks(self):
        for proc in self.proc_running:
            if proc.is_alive():
                continue

            self.proc_running.remove(proc)

            task = proc.task
            try:
                result = proc.queue.get_nowait()
            except Empty:
                result = Task.TaskResult(
                    status=Task.Status.FAILURE,
                    output=None,
                    exception=Exception("Unknown error"),
                    traceback=None,
                )

            task.set_status(result.status)
            task.set_output(result.output)

            if result.status == Task.Status.SUCCESS:
                self.tasks_finished.add(task)
            else:
                self.tasks_error.add(task)
                # raise result.exception

            # self.status_image(f"{self.data_path}/img/status-{time.time()}.png")

    def set_pipeline(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.G = pipeline.create_graph()

    def status_image(self, fname):
        img = self.pipeline.plot_graph()
        with open(fname, "wb") as f:
            f.write(img)

    def generate_run_name(self):
        dtstr = datetime.now().strftime("%y%m%dT%H%M%S")
        return self.pipeline.name + "_" + dtstr

    def run(self, pipeline):

        self.set_pipeline(pipeline)
        self.tasks_queue = list(nx.topological_sort(self.G))

        run_name = self.generate_run_name()
        output_path = os.path.join(self.log_path, run_name)
        os.mkdir(output_path)

        while len(self.tasks_queue) > 0 or len(self.proc_running) > 0:
            if len(self.proc_running) < self.process_limit:
                task = self.dequeue()
                if task is not None:
                    proc = self.start_task(task)
                    self.proc_running.append(proc)

            self.finish_tasks()

            time.sleep(0.01)

        self.status_image(f"{output_path}/status.png")


def main():

    p = Pipeline("hallo")
    d1 = p(LoadDataTask("covid", case_type="covid", wait=10))
    d2 = p(LoadDataTask("influenza", case_type="influenza", wait=1))

    p1 = d1(ProcessDataTask("covid", param="params1", other_param="2", wait=1))
    p2 = d2(ProcessDataTask("influenza", param="params2", wait=1))
    p2 = p2(ProcessDataTask("influenza2", param="params4", wait=1))
    e = EvaluateDataTask("both")
    p1(e)
    p2(e)

    runner = Runner(data_path="../data/", log_path="../log/")
    runner.run(p)


if __name__ == "__main__":
    main()
