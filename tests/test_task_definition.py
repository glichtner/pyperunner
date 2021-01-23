import pytest

from pyperunner import Task, Pipeline, Runner, task, TaskError


def test_node_not_initiated_error(pipeline, task_simple):
    with pytest.raises(ValueError, match=r"Supplied node is not instantiated"):
        pipeline.add(task_simple)


def test_node_invalid_error(pipeline):
    def invalid_task():
        pass

    with pytest.raises(ValueError, match=r"Supplied node is not of type Node"):
        pipeline.add(invalid_task)


def test_task_without_data_parameter(pipeline, runner):

    with pytest.raises(
        AttributeError,
        match=r"To receive input data, the function must accept the named parameter \"data\"",
    ):

        @task("Task")
        def task_wo_data_error():
            pass

    @task("Task", receives_input=False)
    def task_wo_data():
        pass


def test_task_with_data_parameter(pipeline, runner):
    @task("SimpleTask")
    def simpletask(data):
        return "simple-task"


def test_task_with_parameter_not_set():
    task_name = "TaskWithParameter"

    @task(task_name)
    def simpletask(data, n_bytes, see):
        return n_bytes

    with pytest.raises(
        TaskError,
        match=rf"Could not create task {task_name}\(\): missing a required argument: \'n_bytes\'",
    ):
        simpletask()

    with pytest.raises(
        TaskError,
        match=rf"Could not create task {task_name}\(\): got an unexpected keyword argument 'wrong_param'",
    ):
        simpletask(n_bytes=20, see=30, wrong_param=20)


def test_task_class_without_run_method():
    class TestTask(Task):
        pass

    with pytest.raises(
        TypeError,
        match="Can't instantiate abstract class TestTask with abstract methods run",
    ):
        TestTask()


def test_task_class_without_run_decorator(pipeline, runner):
    class TestTask(Task):
        def run(self, **kwargs):
            pass

    with pytest.raises(
        TaskError, match=r"TestTask.run\(\) method not decorated with @run"
    ):
        TestTask()


def test_task_no_name():
    with pytest.raises(
        TypeError, match=r"task\(\) missing 1 required positional argument: 'name'"
    ):

        @task(receives_input=False)
        def test():
            pass

        test()

    with pytest.raises(
        TypeError,
        match=r"Invalid decorator use \- did you use @task instead of @task\(name\)\?",
    ):

        @task
        def test():
            pass

        test()
