import logging
from datetime import datetime
from queue import Empty
from typing import List, Set, Any, Union, Dict
import os
import sys
import multiprocessing
import time

import networkx as nx

# inspired by https://github.com/ecdavis/multiprocessing_dag
from .logger import init_logger, StreamLogger
from ..pipeline import Task, Pipeline


class Process(multiprocessing.Process):
    def __init__(self, task: Task, queue: multiprocessing.Queue):
        super().__init__()

        self.task = task
        self.queue = queue
        self.logger = logging.getLogger(task.name)

    def _get_input(self) -> Any:
        try:
            input = self.queue.get_nowait()
        except Empty:
            input = None

        return input

    def run(self) -> None:
        sys.stdout = StreamLogger(self.logger, logging.INFO)  # type: ignore
        sys.stderr = StreamLogger(self.logger, logging.ERROR)  # type: ignore

        input = self._get_input()

        try:
            self.task.run(input)
        except Exception as e:
            raise e
        finally:
            self.queue.put(self.task.result)
            self.queue.close()


class Runner:
    def __init__(
        self,
        data_path: str,
        log_path: str,
        process_limit: int = multiprocessing.cpu_count(),
    ):
        self.tasks_finished: Set[Task] = set()
        self.tasks_error: Set[Task] = set()
        self.tasks_queue: List[Task] = []
        self.proc_running: List[Process] = []

        self.process_limit: int = process_limit
        self.pipeline: Pipeline
        self.g: nx.DiGraph
        self.logger: logging.Logger

        if not os.path.exists(data_path):
            os.makedirs(data_path)
        self.data_path: str = data_path

        if not os.path.exists(log_path):
            os.makedirs(log_path)
        self.log_path: str = log_path
        self.log_path_current_run: str = log_path

    def dequeue(self) -> Union[Task, None]:
        for task in self.tasks_queue:
            predecessor_tasks = set(self.g.predecessors(task))
            if predecessor_tasks & set(self.tasks_error):
                # this task cannot be run anymore as at least on predecessor failed
                task.set_status(Task.Status.CANT_RUN)
                self.tasks_queue.remove(task)
                self.tasks_error.add(
                    task
                )  # required to pass unreachability further upstream
            if predecessor_tasks.issubset(self.tasks_finished):
                self.tasks_queue.remove(task)
                return task
        return None

    def get_predecessor_outputs(self, task: Task) -> Dict[str, Task]:
        return {str(pt): pt.output for pt in self.g.predecessors(task)}

    def start_task(self, task: Task, force_reload: bool) -> Process:
        queue: multiprocessing.Queue = multiprocessing.Queue()
        queue.put(self.get_predecessor_outputs(task))
        task.set_data_path(self.data_path)

        if force_reload:
            task.set_reload(force_reload)

        proc = Process(task, queue)
        proc.start()
        task.set_status(Task.Status.RUNNING)

        return proc

    def finish_tasks(self) -> None:
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
                )

            task.set_status(result.status)
            task.set_output(result.output)

            if result.status == Task.Status.SUCCESS:
                self.tasks_finished.add(task)
            else:
                self.tasks_error.add(task)
                self.log_exception(result)

    def log_exception(self, result: Task.TaskResult) -> None:
        filename = os.path.join(self.log_path_current_run, "exception.log")
        with open(filename, "a") as f:
            f.write(result.traceback)
            f.write("\n")

    @staticmethod
    def validate_pipeline(pipeline: Pipeline) -> None:
        pipeline.assert_unique_nodes()
        pipeline.assert_acyclic()

    def set_pipeline(self, pipeline: Pipeline) -> None:
        self.pipeline = pipeline
        self.g = pipeline.create_graph()

    def write_status_image(self, fname: str = "status.png") -> None:
        img = self.pipeline.plot_graph()
        path = os.path.join(self.log_path_current_run, fname)
        with open(path, "wb") as f:
            f.write(img)

    def generate_run_name(self) -> str:
        dtstr = datetime.now().strftime("%y%m%dT%H%M%S")
        return self.pipeline.name + "_" + dtstr

    def run(self, pipeline: Pipeline, force_reload: bool = False) -> None:

        self.validate_pipeline(pipeline)
        self.set_pipeline(pipeline)
        self.tasks_queue = list(nx.topological_sort(self.g))

        run_name = self.generate_run_name()
        self.log_path_current_run = os.path.join(self.log_path, run_name)
        os.mkdir(self.log_path_current_run)

        pipeline.to_file(os.path.join(self.log_path_current_run, "pipeline.yaml"))
        self.logger = init_logger(log_path=self.log_path_current_run)

        while len(self.tasks_queue) > 0 or len(self.proc_running) > 0:
            if len(self.proc_running) < self.process_limit:
                task = self.dequeue()
                if task is not None:
                    proc = self.start_task(task, force_reload)
                    self.proc_running.append(proc)

            self.finish_tasks()

            time.sleep(0.01)

        self.write_status_image()
