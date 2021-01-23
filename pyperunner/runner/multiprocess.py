import logging
from datetime import datetime
from queue import Empty
from typing import List, Set, Any, Union, Dict, Generator, Callable, Optional
import os
import sys
import multiprocessing
import time

import networkx as nx
import yaml

from pyperunner.task import Task
from pyperunner.pipeline import Pipeline, PipelineError
from pyperunner.runner.logger import init_logger, StreamLogger
from pyperunner.dag import draw
from pyperunner.util import PipelineResult
from pyperunner.environment import get_environment_info, get_host_info


# inspired by https://github.com/ecdavis/multiprocessing_dag
class Process(multiprocessing.Process):
    """
    Encapsulates the run logic of each task

    Args:
        task: Task to run
        data: Input data for the task
        result_queue: Queue to which the task's return data is written
    """

    def __init__(self, task: Task, data: Any, result_queue: multiprocessing.Queue):
        super().__init__()

        self.task: Task = task
        self.data: Any = data
        self.result_queue: multiprocessing.Queue = result_queue
        self.logger: logging.Logger = logging.getLogger(task.name)
        self.result_received = False

    def run(self) -> None:
        """
        Forks the process and starts execution of the task

        Returns: None

        """
        sys.stdout = StreamLogger(self.logger, logging.INFO)  # type: ignore
        sys.stderr = StreamLogger(self.logger, logging.ERROR)  # type: ignore

        try:
            self.task.run(data=self.data)
        except Exception as e:
            raise e
        finally:
            self.result_queue.put(self.task.result)
            self.result_queue.close()
            self.logger.debug("result_queue closed")


# TODO: Create baserunner, subclass multiprocess runner and create sequential runner (single process)
class Runner:
    """
    Pipeline runner with multiprocessing.

    This runner executes all tasks of a given pipeline in order. Each task is started as a separate process, forked from
    the main process (via `multiprocessing.Process`, see :py:class:`~pyperunner.runner.multiprocess.Process`). If possible, tasks are run
    in parallel.
    This is generally possible for tasks that do not depend on each other in any way (i.e. are not predecessors or
    successors of one another). The maximum number of concurrrent processes is limited via the process_limit` parameter.

    Args:
        data_path: Path where data will be stored
        log_path: Path where log files will be stored
        process_limit: Maximum number of concurrent worker processes
    """

    def __init__(
        self, data_path: str, log_path: str, process_limit: int = None,
    ) -> None:
        self.tasks_finished: Set[Task] = set()
        self.tasks_error: Set[Task] = set()

        self.tasks_queue: List[Task] = []
        """
        A list of all tasks in order (topological sorting of the underlying DAG)
        """

        self.tasks_execute: List[Task] = []
        """
        A list of tasks that need to be executed
        """

        self.proc_running: List[Process] = []

        if process_limit is None:
            process_limit = multiprocessing.cpu_count()

        self.process_limit: int = process_limit
        self.pipeline: Pipeline
        self.g: nx.DiGraph = nx.DiGraph()
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
        """
        Get a runnable Task from the queue.

        Loops through the task queue and evaluates for each task
        - if any of the predecessor tasks failed
        - if all of the predecessor tasks have finished

        In the former cases (failure of predecessors), the task is removed from the task list and added to the failed
        task list itself (required to propagate the error).
        In the latter case (all predecessors finished), the task is ready to run and as such removed from the queue
        and returned by the function.
        Otherwise the function returns None.

        Returns: Runnable task, if any, else None

        """
        for task in self.tasks_queue:
            predecessor_tasks = set(self.g.predecessors(task))
            if predecessor_tasks & set(self.tasks_error):
                # this task cannot be run anymore as at least on predecessor failed
                task.set_status(Task.Status.CANT_RUN)
                self.tasks_queue.remove(task)
                self.tasks_error.add(
                    task
                )  # required to pass unreachability further upstream
                # TODO: This could be directly done (loop through all child tasks of a failed task)
            if predecessor_tasks.issubset(self.tasks_finished):
                self.tasks_queue.remove(task)
                return task
        return None

    def get_predecessor_outputs(self, task: Task) -> List[Any]:
        """
        Get the results of all predecessor (parent) task of a task.

        Used to feed the outputs (return values) of the parent task to a child class when running it.

        Args:
            task: Task for which the predecessor's outputs are returned.

        Returns: List of all parent task outputs

        """
        return [pt.output for pt in self.g.predecessors(task)]

    def start_task(self, task: Task) -> Process:
        """
        Run a task.

        Initiates the Process object for the task class and starts it by forking (see :py:meth:`Process.run`). :py:class:`Process`

        Args:
            task: Task to start

        Returns: Process object of the started task

        """
        queue_out: multiprocessing.Queue = multiprocessing.Queue()

        task.set_data_path(self.data_path)

        proc = Process(task, self.get_predecessor_outputs(task), queue_out)
        proc.start()
        task.set_status(Task.Status.RUNNING)

        return proc

    def skip_task(self, task: Task) -> None:
        """
        Skips execution of a task.

        Used for tasks that are neither required to run nor required to load data from (they are completely bypassed).
        E.g. in a pipeline of tasks a -> b -> c, if only c needs to be run (because it wasn't run so far or because
        it is forced to run setting reload=True in :py:class:`~pyperunner.Task()`

        Args:
            task: Task to skip

        Returns: None

        """
        result = Task.TaskResult(Task.Status.SKIPPED, output=None)
        self.process_task_result(task, result)

    def process_task_result(self, task: Task, result: Task.TaskResult) -> None:
        """
        Process the result object of a task.

        When a task has finished running or does not need to be run at all, this function performs the required
        internal status maintenance:

        - Set the task result and status to those of the result object
        - Add the task to the finished or the failed tasks and log exceptions, if necessary

        Args:
            task: Task of which the result should be processed
            result: Result of the given task

        Returns: None

        """
        task.set_status(result.status)
        task.set_output(result.output)
        task.result = result

        if result.status != Task.Status.FAILURE:
            self.tasks_finished.add(task)
        else:
            self.tasks_error.add(task)
            self.log_exception(result)

    def finish_tasks(self) -> None:
        """
        Clean up after tasks have finished.

        Loops through processes that have started but not been removed from the running processes list yet.
        Next, the following steps are performed

        - If process is dead, it is removed from the list of running tasks
            - If the processed exited with an error exit code (e.g. out of memory error), that exception is logged
        - Data from the result queue is tried to be read (independent of whether the process is dead or alive)
            - If data could be read from the queue, the :py:meth:`Runner.process_task_result` function is called
            - If no data could be read
                - if the process is still alive, nothing happens
                - if the process is dead, an exception is raised

        Returns: None

        """
        for proc in self.proc_running:
            self._finish_task(proc)

    def _receive_queue(self, proc: Process, proc_removed: bool) -> bool:
        """
        Pulls a process (task) queue for return data.

        Args:
            proc: Process to query for data
            proc_removed: If the process has been removed, i.e. is dead

        Returns: True if data was received or process is dead and no data was received, False otherwise

        """
        try:
            result = proc.result_queue.get_nowait()
            proc.result_received = True
        except Empty:
            # No result received from the process
            if not proc_removed:
                # The process is still alive and wasn't removed, so just continue (this is a normal status)
                return False
            else:
                # Process was removed (i.e. is dead) but didn't yield a result -> exception state, because
                # if an error/exception had occurred during the process it would have been captured by
                # TODO document by which part normal exceptions during task execution are captured
                result = Task.TaskResult(
                    status=Task.Status.FAILURE,
                    output=None,
                    exception=Exception("Unknown error"),
                )

        self.process_task_result(proc.task, result)

        return True

    def _finish_task(self, proc: Process) -> None:
        """
        Finishes a single task (process).

        See :py:meth:`pyperunner.Runner.finish_tasks` for details.

        Args:
            proc: Process to finish

        Returns: None

        """
        if proc.is_alive():
            self._receive_queue(proc, proc_removed=False)
        else:
            self.proc_running.remove(proc)
            self.logger.debug(f"{proc.task.name} exit code: {proc.exitcode}")

            if proc.exitcode == 0:
                # Task exited normally
                if not proc.result_received:
                    self._receive_queue(proc, proc_removed=True)
            else:
                # Task exited with an error
                if not proc.result_received:
                    results_received = self._receive_queue(proc, proc_removed=True)
                    if not results_received:
                        e = Exception(
                            f"Task {proc.task.name} exited with code {proc.exitcode}"
                        )
                        result = Task.TaskResult(
                            status=Task.Status.FAILURE, output=None, exception=e,
                        )
                        self.logger.error(str(e))
                        self.process_task_result(proc.task, result)

    def log_exception(self, result: Task.TaskResult) -> None:
        """
        Provide exception info from a task result to the logger.

        Called when processing task results (:py:meth:`Runner.process_task_result`)

        Args:
            result: Task result

        Returns: None

        """
        filename = os.path.join(self.log_path_current_run, "exception.log")
        with open(filename, "a") as f:
            f.write(result.traceback)
            f.write("\n")

    @staticmethod
    def validate_pipeline(pipeline: Pipeline) -> None:
        """
        Asserts a pipeline conforms to requirements.

        Requirements
        - Every node in the pipeline must have a unique name
        - The pipeline must be acyclic

        Raises exceptions if one of the requirements isn't met by the supplied pipeline.

        Args:
            pipeline: Pipeline to check

        Returns: None

        """
        if not isinstance(pipeline, Pipeline):
            raise TypeError(f"pipeline must be of type Pipeline, not {type(pipeline)}")
        pipeline.assert_unique_nodes()
        pipeline.assert_acyclic()

    def assert_valid_pipeline(self) -> None:
        """
        Asserts that the pipeline attribute of this object is set to a valid pipeline object.

        Returns: None

        """
        if self.pipeline is None:
            raise ValueError("No pipeline supplied")

        if not isinstance(self.pipeline, Pipeline):
            raise ValueError("Pipeline parameter must be an instance of Pipeline")

        self.validate_pipeline(self.pipeline)

    def set_pipeline(self, pipeline: Pipeline) -> None:
        """
        Sets the pipeline to be run by the runner.

        Args:
            pipeline: Pipeline to be run

        Returns: None

        """
        self.validate_pipeline(pipeline)
        self.pipeline = pipeline
        self.g = pipeline.create_graph()

    def write_status_image(self, fname: str = "status.png") -> None:
        """
        Write an image of the pipeline's DAG to disk.

        Creates a PNG image of each of the node and the connections of the DAG associated with the pipeline
        and writes that to disk

        Args:
            fname: Filename to write to (PNG)

        """
        self.assert_valid_pipeline()

        img = self.pipeline.plot_graph()
        path = os.path.join(self.log_path_current_run, fname)
        with open(path, "wb") as f:
            f.write(img)

    def generate_run_name(self) -> str:
        """
        Get a unique name for the current run of the pipeline.

        Used to create the run-specific log path

        Returns: pipeline name + current timestamp
            e.g. "<my-pipeline_210119T222719"

        """
        self.assert_valid_pipeline()

        dtstr = datetime.now().strftime("%y%m%dT%H%M%S")

        return self.pipeline.name + "_" + dtstr

    def pipeline_params_filename(self) -> str:
        """
        Get filename of pipeline parameter yaml file.

        See :py:meth:`Runner.save_pipeline_params`

        Returns: Filename of pipeline parameter yaml file

        """
        return os.path.join(self.log_path_current_run, "pipeline.yaml")

    def save_pipeline_params(self) -> None:
        """
        Stores a complete description of the current pipeline run to a yaml file.

        Parameter file contains
        - run specific information (paths, environment) (see py:func:`pyperunner.environment.get_environment_info`)
        - name, parameter and parents of each task

        Returns: None

        """
        self.assert_valid_pipeline()

        pdict = self.pipeline.to_dict()
        pdict["run"] = {
            "data_path": self.data_path,
            "log_path": self.log_path,
            "host": get_host_info(),
            "env": get_environment_info(stack_level=3),
        }

        filename = self.pipeline_params_filename()

        with open(filename, "w") as f:
            yaml.dump(pdict, f)

    def results(self) -> PipelineResult:
        """
        Get the :py:class:`~pyperunner.PipelineResult` object for the current run.

        Returns: PipelineResult object for current run

        """

        return PipelineResult.from_file(self.pipeline_params_filename())

    def _get_nodes_to_execute(self) -> Generator:
        """
        Yield all tasks that need to be executed in order of possible execution.

        A node will be returned if:

        - it is marked to be run
        - any of its successors (children) is marked to be run (because in this case the result of the node is required
          for the children).

        Returns: Node to be executed.

        """
        for node in nx.topological_sort(self.g):
            if node.should_run() or any(
                child.should_run() for child in self.g.successors(node)
            ):
                yield node

    def analyze_pipeline(self, force_reload: bool = False) -> None:
        """
        Marks all nodes (tasks) in the pipeline that need to be run.

        A node is required to run if:

        - force_reload is True
        - the node itself is marked to be run (i.e., the node was created using reload=True in :py:class:`~pyperunner.Task()`
          or there is no cached result for the node)
        - any predecessor node is required to run

        Args:
            force_reload: If true, all nodes are marked to be run

        Returns: None

        """
        self.assert_valid_pipeline()

        g = self.pipeline.create_graph()
        for task in g.nodes:
            task.set_data_path(self.data_path)

        reload_nodes = [n for n in self.g.nodes if n.should_run() or force_reload]

        def set_reload(node: Task) -> None:
            node.reload = True
            for s in self.g.successors(node):
                set_reload(s)

        for node in reload_nodes:
            set_reload(node)

    def queue_tasks(self, force_reload: bool = False) -> None:
        """
        Fills lists of tasks that need to be executed.

        Note that there are two lists:

        - :py:attr:`~Runner.tasks_queue` A list of all tasks in order (topological sorting of the underlying DAG)
        - :py:attr:`~Runner.tasks_execute` A list of tasks that need to be executed

        Tasks do not need to be executed when their result is cached and the result is not required in a direct child
        task.

        Args:
            force_reload: If true, all nodes are marked to be run

        Returns: None

        """
        self.analyze_pipeline(force_reload)
        self.tasks_execute = list(self._get_nodes_to_execute())
        self.tasks_queue = list(nx.topological_sort(self.g))

    def execution_plan_summary(self, print_fn: Optional[Callable] = None) -> None:
        """
        Print execution summary of the underlying pipeline DAG in ASCII format.

        Note that this may easily take a lot of vertical space if multiple tasks are run in parallel and may
        not be readable anymore.

        Args:
            print_fn: Function that prints the execution summary (default: print)

        Returns: None

        """

        self.queue_tasks()

        def node_name(n: Task) -> str:
            name = n.name
            if n in self.tasks_execute:
                name += "*"
            return name

        s = draw(
            [node_name(n) for n in self.g.nodes],
            [(node_name(t), node_name(s)) for s, t in self.g.edges],
        )

        if print_fn is None:
            print_fn = print

        print_fn(s)

    def run(
        self,
        pipeline: Optional[Pipeline] = None,
        force_reload: bool = False,
        show_execution_plan: bool = False,
    ) -> None:
        """
        Run a pipeline.

        Main function of the runner. Evaluates which tasks to run and executes them in order. Each task is run in
        a separate process. Results from each task are collected using multiprocessing.Queue.

        Call :py:meth:`Runner.results` after finishing the run to access the results or alternatively create
        a :py:attr:`pyperunner.PipelineResult` object explicitly from the parameter.yaml file of the run.

        Args:
            pipeline: pipeline to be run (or None, if already set via :py:meth:`Runner.set_pipeline`)
            force_reload: Set true if all tasks should run even if they would not need to
            show_execution_plan: Set true if an execution summary of the underlying pipeline DAG should be displayed.

        Returns: None

        """

        if pipeline is not None:
            self.set_pipeline(pipeline)

        self.assert_valid_pipeline()

        if len(pipeline) == 0:  # type: ignore
            raise PipelineError("No tasks in pipeline")

        self.queue_tasks(force_reload)

        if show_execution_plan:
            self.execution_plan_summary()

        run_name = self.generate_run_name()
        self.log_path_current_run = os.path.join(self.log_path, run_name)
        os.mkdir(self.log_path_current_run)

        self.logger = init_logger(
            log_path=self.log_path_current_run, log_level=logging.INFO
        )

        self.save_pipeline_params()
        self.logger.info(
            f"Storing pipeline parameters in {self.pipeline_params_filename()}"
        )
        self.logger.info(f"Storing pipeline data in {self.data_path}")

        while len(self.tasks_queue) > 0 or len(self.proc_running) > 0:
            if len(self.proc_running) < self.process_limit:
                task = self.dequeue()
                if task is not None:

                    if task not in self.tasks_execute:
                        self.logger.info(
                            f'No need to execute task "{task.name}", skipping it'
                        )
                        self.skip_task(task)
                    else:
                        proc = self.start_task(task)
                        self.proc_running.append(proc)

            self.finish_tasks()

            time.sleep(0.01)

        pipeline.set_results(self.results())  # type: ignore

        if len(self.tasks_error) == 0:
            self.logger.info("Pipeline run finished")
        else:
            self.logger.warning("Pipeline run finished with errors")

        self.write_status_image()
        self.logger.info(
            f"Stored pipeline parameters in {self.pipeline_params_filename()}"
        )
