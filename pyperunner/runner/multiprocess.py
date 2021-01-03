import gc
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
from dask.tests.test_config import yaml

from .logger import init_logger, StreamLogger
from ..pipeline import Pipeline
from ..util import PipelineResult
from ..task import Task


class Process(multiprocessing.Process):
    def __init__(self, task: Task, data: Any, result_queue: multiprocessing.Queue):
        super().__init__()

        self.task: Task = task
        self.data: Any = data
        self.result_queue: multiprocessing.Queue = result_queue
        self.logger: logging.Logger = logging.getLogger(task.name)

    def run(self) -> None:
        sys.stdout = StreamLogger(self.logger, logging.INFO)  # type: ignore
        sys.stderr = StreamLogger(self.logger, logging.ERROR)  # type: ignore

        try:
            self.task.run(self.data)
        except Exception as e:
            raise e
        finally:
            self.result_queue.put(self.task.result)
            self.result_queue.close()
            self.logger.debug("result_queue closed")


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

        data_path = os.path.realpath(data_path)
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        self.data_path: str = data_path

        log_path = os.path.realpath(log_path)
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
        return {pt.name: pt.output for pt in self.g.predecessors(task)}

    def start_task(self, task: Task, force_reload: bool) -> Process:
        queue_out: multiprocessing.Queue = multiprocessing.Queue()

        task.set_data_path(self.data_path)

        if force_reload:
            task.set_reload(force_reload)

        proc = Process(task, self.get_predecessor_outputs(task), queue_out)
        proc.start()
        task.set_status(Task.Status.RUNNING)

        return proc

    def process_task_result(self, proc: Process, result: Task.TaskResult) -> None:
        task = proc.task
        task.set_status(result.status)
        task.set_output(result.output)

        if result.status == Task.Status.SUCCESS:
            self.tasks_finished.add(task)
        else:
            self.tasks_error.add(task)
            self.log_exception(result)

    def finish_tasks(self) -> None:
        for proc in self.proc_running:

            proc_removed = False

            if not proc.is_alive():
                self.proc_running.remove(proc)
                proc_removed = True
                # TODO make sure task has provided data
                self.logger.debug(f"{proc.task.name} exit code: {proc.exitcode}")

                if proc.exitcode != 0:
                    e = Exception(
                        f"Task {proc.task.name} exited with code {proc.exitcode}"
                    )
                    result = Task.TaskResult(
                        status=Task.Status.FAILURE, output=None, exception=e,
                    )
                    self.logger.error(str(e))
                    self.process_task_result(proc, result)
                    continue

                if proc.task.status != Task.Status.RUNNING:
                    # the status of this task was already updated, no need to continue this loop
                    continue

            try:
                result = proc.result_queue.get_nowait()
            except Empty:
                if not proc_removed:
                    continue
                else:
                    result = Task.TaskResult(
                        status=Task.Status.FAILURE,
                        output=None,
                        exception=Exception("Unknown error"),
                    )

            self.process_task_result(proc, result)

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

    def pipeline_params_filename(self) -> str:
        return os.path.join(self.log_path_current_run, "pipeline.yaml")

    def save_pipeline_params(self) -> None:
        pdict = self.pipeline.to_dict()
        pdict["run"] = {"data_path": self.data_path, "log_path": self.log_path}

        filename = self.pipeline_params_filename()

        with open(filename, "w") as f:
            yaml.dump(pdict, f)

    def results(self) -> PipelineResult:
        return PipelineResult.from_file(self.pipeline_params_filename())

    def run(self, pipeline: Pipeline, force_reload: bool = False) -> None:

        self.validate_pipeline(pipeline)
        self.set_pipeline(pipeline)
        self.tasks_queue = list(nx.topological_sort(self.g))

        run_name = self.generate_run_name()
        self.log_path_current_run = os.path.join(self.log_path, run_name)
        os.mkdir(self.log_path_current_run)

        self.save_pipeline_params()

        self.logger = init_logger(
            log_path=self.log_path_current_run, log_level=logging.INFO
        )

        while len(self.tasks_queue) > 0 or len(self.proc_running) > 0:
            if len(self.proc_running) < self.process_limit:
                task = self.dequeue()
                if task is not None:
                    proc = self.start_task(task, force_reload)
                    self.proc_running.append(proc)

            self.finish_tasks()

            time.sleep(0.01)

        self.write_status_image()
