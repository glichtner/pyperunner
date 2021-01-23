from typing import Dict, Any, Generator
import os
import joblib
import yaml


class TaskResult:
    """
    Access to the output of a single task

    Args:
        name: Task name
        data_path: Path where the results are stored
        conf: Configuration as saved by the task during running
    """

    def __init__(self, name: str, data_path: str, conf: Dict) -> None:
        self.name = name
        self.data_path = data_path
        self.conf = conf
        self.hash = conf["hash"]

    def set_data_path(self, data_path: str) -> None:
        """
        Sets the data path of the task

        Args:
            data_path: Path where the results are stored

        Returns: None

        """
        self.data_path = data_path

    def output_filename(self) -> str:
        """
        Generate the output filename of the task based on the task name and its hash

        Returns: Output filename of the task's result

        """

        return os.path.join(self.data_path, self.name, self.hash, "result.dump.gz")

    def output(self) -> Any:
        """
        Load the task's output

        Returns: Task's stored output

        """
        return joblib.load(self.output_filename())

    @staticmethod
    def from_file(filename: str) -> "TaskResult":
        """
        Create a TaskResult object from the params.yaml file saved along the task results

        Args:
            filename: params.yaml file saved along the task results

        Returns: Task result object

        """
        data_path = os.path.realpath(
            os.path.join(os.path.dirname(filename), "..", "..")
        )

        with open(filename, "r") as f:
            task_conf = yaml.safe_load(f)

        task_name = task_conf["task"]["name"]
        task_tag = task_conf["task"]["tag"]

        return TaskResult(f"{task_name}({task_tag})", data_path, task_conf["task"])


class PipelineResult:
    """
    Accessor of task results of a pipeline run.

    Instantiate using the pipeline run configuration or using the from_file method pointing to the parameter.yaml file
    saved during the pipeline run. Then iterate over the single tasks results or access single task results using
    array indexing:

    .. code-block:: python

        results = PipelineResult.from_file(fname)

        # loop through individual task results
        for task_name in pipeline.results():
            print(pipeline[task_name])

    Args:
        conf: Configuration (pipeline.yaml saved in the pipeline run log dir)
        data_path: Path where the results are stored
    """

    def __init__(self, conf: Dict, data_path: str = "") -> None:
        self.tasks = {}

        if data_path == "":
            if "run" not in conf.keys():
                raise ValueError(
                    'A "run" entry in the configuration is required to infer the data path.'
                )
            self.data_path = conf["run"]["data_path"]
        else:
            self.data_path = data_path

        self.conf = conf

        for task in self.conf["tasks"]:
            self.tasks[task] = TaskResult(
                task, self.data_path, self.conf["tasks"][task]
            )

    def set_data_path(self, data_path: str) -> None:
        """
        Sets the data path of the task

        Args:
            data_path: Path where the results are stored

        Returns: None

        """
        self.data_path = data_path
        for task_name in self.tasks:
            self.tasks[task_name].set_data_path(data_path)

    def task_result(self, task: str) -> Any:
        """
        Return the results of a certain task.

        Args:
            task: Task name

        Returns: Results of the task

        """
        return self.tasks[task].output()

    def __getitem__(self, task: str) -> Any:
        return self.task_result(task)

    def __iter__(self) -> Generator:
        for key in self.tasks.keys():
            yield key

    @staticmethod
    def from_file(filename: str) -> "PipelineResult":
        """
        Create a PipelineResult object from the parameter.yaml file saved in the pipeline run log path

        Args:
            filename: parameter.yaml file saved in the pipeline run log path

        Returns: PipelineResult object

        """
        with open(filename, "r") as f:
            pipeline_dict = yaml.safe_load(f)
        return PipelineResult(pipeline_dict)
