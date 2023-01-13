import numpy as np
import pytest

from pyperunner import Pipeline, Runner, Task, run, task


@pytest.fixture
def pipeline():
    return Pipeline("test-pipeline")


@pytest.fixture
def runner(tmp_path):
    return Runner(data_path=tmp_path / "data", log_path=tmp_path / "log")


@pytest.fixture
def task_simple():
    @task("SimpleTask")
    def simpletask(data):
        return "simple-task"

    return simpletask


@pytest.fixture
def task_simple_no_input():
    @task("SimpleTask", receives_input=False)
    def simpletask():
        return "simple-task"

    return simpletask


@pytest.fixture
def task_large_data():
    class LargeDataTask(Task):
        @run
        def run(self, data, n_bytes):
            arr = np.ones(shape=(n_bytes, 1), dtype=np.uint8)
            assert arr.nbytes == n_bytes

            return arr

    return LargeDataTask
