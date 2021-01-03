from typing import Dict, Any, Generator
import os
import joblib
import yaml


class TaskResult:
    def __init__(self, name: str, data_path: str, conf: Dict) -> None:
        self.name = name
        self.data_path = data_path
        self.conf = conf
        self.hash = conf["hash"]

    def output_filename(self) -> str:
        return os.path.join(self.data_path, self.name, self.hash, "result.dump.gz")

    def output(self) -> Any:
        return joblib.load(self.output_filename())

    @staticmethod
    def from_file(filename: str) -> "TaskResult":
        data_path = os.path.realpath(
            os.path.join(os.path.dirname(filename), "..", "..")
        )

        with open(filename, "r") as f:
            task_conf = yaml.safe_load(f)

        task_name = task_conf["task"]["name"]
        task_tag = task_conf["task"]["tag"]

        return TaskResult(f"{task_name}({task_tag})", data_path, task_conf["task"])


class PipelineResult:
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

        for task in conf["tasks"]:
            self.tasks[task] = TaskResult(task, data_path, conf["tasks"][task])

    def task_result(self, task: str) -> Any:
        return self.tasks[task].output()

    def __getitem__(self, task: str) -> Any:
        return self.task_result(task)

    def __iter__(self) -> Generator:
        for key in self.tasks.keys():
            yield key

    @staticmethod
    def from_file(filename: str) -> "PipelineResult":
        with open(filename, "r") as f:
            pipeline_dict = yaml.safe_load(f)
        return PipelineResult(pipeline_dict)
