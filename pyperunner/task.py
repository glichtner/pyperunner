import inspect
from typing import Any, Optional, Dict, List, Union, Callable, Iterator
import hashlib
import json
import logging
import os
from abc import abstractmethod, ABCMeta
from dataclasses import dataclass
from enum import Enum
import traceback

import joblib
import yaml

from pyperunner.dag import Node


class TaskError(Exception):
    pass


def get_exception_str(exc: BaseException) -> str:
    """
    Format the traceback of an exception

    Args:
        exc: Exception

    Returns:  Exception traceback

    """
    s = traceback.format_exception(etype=None, value=exc, tb=exc.__traceback__)
    return "".join(s)


class Result:
    """
    Generic task result (data that is stored on disk)
    """

    pass


class Task(Node, metaclass=ABCMeta):
    """
     A task is a single work unit that is run as part of a :py:class:`~pyperunner.Pipeline`.

     All tasks that are run by a pipeline must subclass this class. There are two main ways to accomplish this:

     1. Using the :py:func:`~pyperunner.task` function decorator:

     .. code-block:: python

         from pyperunner import task

         @task("Hello")
         def hello():
             print("in hello()")
             return "Hello"

     Note that in this case you need to explicitly state the name of the task as a parameter to the
     :py:func:`~pyperunner.task` function decorator (here: "Hello").

     2. Directly subclassing this class and then using the :py:func:`~pyperunner.run` method decorator on the run()
     function. Note that the abstract :py:meth:`~pyperunner.Task.run` function must be implemented when subclassing.

     .. code-block:: python

         from pyperunner import run

         class World(Task):
             @run
             def run(self, data):
                 return f"{data} world"

     Note that in contrast to the :py:func:`~pyperunner.task` function decorator, you don't need to specify the
     task name here. Instead, the class name (here "World") will be used as the task name.

     Args:
         tag: Tag of the task; can be used to use the same task multiple times in a single pipeline (every instance
           of the task needs to have a different tag then to ensure unique task names)
         reload: Set True if the Task should be run regardless of whether cached results already exist
         **kwargs: Additional task-specific parameters
     """

    class Status(Enum):
        """
        Encodes current Status of a :py:class:`~pyperunner.Task`
        """

        NOT_STARTED = 0  # Task hasn't been started yet
        FAILURE = -1  # Task has failed
        SUCCESS = 1  # Successfully executed
        RUNNING = 2  # Currently running
        CANT_RUN = 3  # Task can't run because a predecessor task failed
        SKIPPED = 4  # Task is completely skipped from execution

    @dataclass
    class TaskResult:
        """
        Result of a task

        Args:
            status: Status code
            output: Data returned by the task
            exception: Exception if one was raised
            traceback: Traceback of an exception, if one was raised
        """

        status: "Task.Status"
        output: Any
        exception: Optional[Exception] = None
        traceback: str = ""

    _run_signature = None

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

        self.assert_run_decorated()
        try:
            self.assert_params_complete()
        except TypeError as e:
            raise TaskError(f"Could not create task {self.task_name}({self.tag}): {e}")

    def assert_run_decorated(self) -> None:
        if (
            not hasattr(self.run, "__decorated__")
            or "run" not in self.run.__decorated__  # type: ignore
        ):
            raise TaskError(f"{self.task_name}.run() method not decorated with @run")

    def assert_params_complete(self) -> None:
        """
        Asserts that the parameter provided in the constructor match those required by the run function.

        This is used to raise TypeError already at task creation time (i.e. early and in main thread),
        not during task execution time.

        Returns: None

        """

        if self._run_signature is None:
            self._run_signature = inspect.signature(self.run)
        params = self.params.copy()
        if "data" in self._run_signature.parameters:
            params["data"] = None
        self._run_signature.bind(**params)

    def _single_node_hash(self) -> str:
        """
        Generates the hash of a single node (i.e. task), constructed from the name and params of the node.

        Returns: Single node hash

        """
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
        """
        Generate the task's hash components

        The hash is constructed from the hash of the specific node and the hashes of *all* parent
        (predecessor) nodes. It is therefore dependent on the pipeline the task is part of.

        Returns: List of this task's hash and all predecessor tasks' hashes.

        """
        hash = [self._single_node_hash() + "_" + self.name]

        for parent in self._parents_generator():
            hash += parent._hash()

        return sorted(hash)

    def hash(self) -> str:
        """
        Generate the task's hash

        The hash is constructed from the hash of the specific node and the hashes of *all* parent
        (predecessor) nodes. It is therefore dependent on the pipeline the task is part of. This is used to precisely
        identify a task in a given context (e.g. when saving and loading pipelines using the
        :py:meth:`pyperunner.Pipeline.to_file` and :py:meth:`pyperunner.Pipeline.from_file` methods to store/load a
        pipeline from file). It ensures reproducibility of single pipeline runs.

        Returns: Task hash

        """
        return hashlib.md5("/".join(self._hash()).encode("utf-8")).hexdigest()

    def description(self) -> Dict[str, Union[str, Dict, List]]:
        """
        Return a complete description of the task.

        The description contains the following properties of the task:

        - name
        - module
        - tag
        - hash (see :py:meth:`~pyperunner.Task.hash`)
        - parameter dictionary

        Returns: Dictionary with information describing the task's configuration

        """
        return {
            "name": self.task_name,
            "module": self.__module__,
            "tag": self.tag,
            "hash": self.hash(),
            "params": self.params,
        }

    def should_run(self) -> bool:
        """
        Determine if the task should run based on the availability of cached data.

        If the task was run previously with the same configuration (see :py:meth:`~pyperunner.Task.description`), the
        output was stored and can be reloaded. In that case, the task does not need to run and this function returns
        False. The task can be forced to run by setting the `reload` parameter to True when instantiating the task
        (see :py:meth:`~pyperunner.Task()`).

        Returns: If the task should be run by the runner

        """
        return not self.output_exists() or self.reload

    @abstractmethod
    def run(self, **kwargs: Dict) -> Any:
        """
        Worker method that must be implemented when subclassing.

        This method contains the Tasks execution logic.
        Note: The

        Args:
            **kwargs: Task-specific parameters

        Returns: The result of the task that is passed onto the successor task(s) (e.g. a pandas DataFrame or
            anything else)

        """
        pass

    def run_wrapper(
        self,
        func: Callable,
        data: Any,
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
                    kwargs["data"] = data
                    # TODO check what happens when the first task is instantiated with "receives_input=True"

                output = func(*args, **kwargs)

                if inspect.isgenerator(output):
                    while True:
                        try:
                            result = next(output)
                            self.store_result(result)
                        except StopIteration as e:
                            output = e.value

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
        """
        Yields every parent task of this task.

        Returns: Parent task

        """
        for p in self.parents:
            if isinstance(p, Task):
                yield p

    def set_data_path(self, path: str) -> None:
        """
        Set the task's data path.

        This is done by the :py:class:`~pyperunner.Runner` when starting the task.

        Args:
            path: Data path

        Returns: None

        """
        self.data_path = path

    def set_output(self, output: Any) -> None:
        """
        Assigns the output generated by the task.

        Args:
            output: Output generated by the task (may be load from disk if it's cached).

        Returns: None

        """
        self.output = output

    def set_status(self, status: "Task.Status") -> None:
        """
        Set the task's status (see :py:class:`~pyperunner.Task.Status`)

        Args:
            status: Task status

        Returns: None

        """
        self.status = status

    def set_reload(self, reload: bool) -> None:
        """
        Sets the reload parameter that specifies whether this task is forced to run even if its output already exists.

        Args:
            reload: If the task should forcibly run even if its output already exists

        Returns: None

        """
        self.reload = reload

    def output_filename(self, filename: str = "result.dump.gz") -> str:
        """
        Get the full path of the file to which the task's output is written.

        The output path is build from the data path (set by the pipeline runner), the task's name and it's hash
        and is therefore specific to a certain configuration of the task (via it's :py:meth:`~pyperunner.Task.hash`).

        Args:
            filename: Filename of the file to which the task's output is written

        Returns: Full path of the file to which the task's output is written

        """
        path = os.path.realpath(os.path.join(self.data_path, self.name, self.hash()))

        if not os.path.exists(path):
            os.makedirs(path)

        return os.path.join(path, filename)

    def output_exists(self) -> bool:
        """
        Returns whether an output of this task already exists (i.e. was cached from a previous run).

        Returns: If the output of this task already exists

        """
        return os.path.exists(self.output_filename())

    def store_output(self, output: Any) -> None:
        """
        Store the output of this task.

        Args:
            output: Output of this task

        Returns: None

        """
        filename = self.output_filename()
        joblib.dump(output, filename)

    def store_result(self, result: Result) -> None:
        """
        Store an intermediate result

        Args:
            result: result to store

        Returns: None

        """
        raise NotImplementedError()

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
        """
        Store the parameters of this task to a params.yaml file

        Returns: None

        """
        filename = self.output_filename("params.yaml")

        params = self._build_caller_dict()

        with open(filename, "w") as f:
            yaml.dump(params, f, default_flow_style=False)

    def load_output(self) -> Any:
        """
        Get the output generated by this task

        Returns: The output generated by this task

        """
        filename = self.output_filename()
        return joblib.load(filename)

    def __str__(self) -> str:
        return f"{self.task_name}({self.tag})#{self.hash()}#{hash(self)}"
