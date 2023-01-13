import networkx as nx
import pytest

from pyperunner import PipelineError, Sequential, Task, run, task


def assert_pipeline_has_run(pipeline, *, order=None, edges=None):

    if order is not None and edges is not None:
        raise AttributeError("Cannot use order and edges at the same time")

    if order is not None:
        g_true = nx.path_graph(order, create_using=nx.DiGraph)
    elif edges is not None:
        g_true = nx.DiGraph()
        g_true.add_edges_from(edges)
    else:
        raise ValueError("Either edges or order must be set")

    g_pipeline = pipeline.create_graph()

    for node_pipeline, node_true in zip(
        nx.topological_sort(g_pipeline), nx.topological_sort(g_true)
    ):
        assert node_pipeline.name == node_true.name

    for node in nx.topological_sort(g_pipeline):
        if node.result.exception is not None:
            raise node.result.exception
        assert node.output_exists()


def test_single_task_pipeline(task_simple_no_input, pipeline, runner):
    ts1 = task_simple_no_input()
    pipeline.add(ts1)
    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, order=[ts1])


def test_second_task_accepts_no_parameter(task_simple_no_input, pipeline, runner):
    ts1 = task_simple_no_input("tag1")
    ts2 = task_simple_no_input("tag2")
    pipeline.add(ts1)
    ts2(ts1)
    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, order=[ts1, ts2])


def test_task_with_input_as_first_task(task_simple, pipeline, runner):
    ts1 = task_simple()
    pipeline.add(ts1)
    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, order=[ts1])


def test_concat_tasks_first_then_add_to_pipeline(task_simple, pipeline, runner):
    ts1 = task_simple("tag1")
    ts2 = task_simple("tag2")
    ts3 = task_simple("tag3")
    ts3(ts2)
    ts2(ts1)
    pipeline.add(ts1)

    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, order=[ts1, ts2, ts3])


def test_first_add_pipeline_then_concat_tasks(
    task_simple_no_input, task_simple, pipeline, runner
):
    ts1 = task_simple("tag1")
    ts2 = task_simple("tag2")
    ts3 = task_simple("tag3")

    pipeline.add(ts1)
    ts2(ts1)
    ts3(ts2)

    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, order=[ts1, ts2, ts3])


def test_multiple_tasks_with_same_name_error(task_simple, pipeline, runner):
    ts_1 = task_simple()
    ts_2 = task_simple()

    pipeline.add(ts_1)
    with pytest.raises(ValueError, match="Node names must be unique"):
        ts_1(ts_2)


def test_pipeline_without_tasks(pipeline, runner):
    with pytest.raises(PipelineError, match="No tasks in pipeline"):
        runner.run(pipeline)


def test_correct_parameter_passing_task(pipeline, runner):

    task1_params = dict(
        task1_param3="task1_param3",
        task1_param1="task1_param1",
        task1_param2="task1_param2",
    )
    task2_params = dict(task2_param1="task2_param1")

    @task("test1", receives_input=False)
    def task1(task1_param1, task1_param2, task1_param3):
        assert task1_param1 == "task1_param1"
        assert task1_param2 == "task1_param2"
        assert task1_param3 == "task1_param3"

        return [task1_param1, task1_param2, task1_param3]

    @task("test2")
    def task2(data, task2_param1):
        assert set(data[0]) == set(task1_params.values())
        assert task2_param1 == "task2_param1"

        return data[0]

    t1 = task1(**task1_params)
    t2 = task2(**task2_params)

    t2(t1)
    pipeline.add(t1)
    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, order=[t1, t2])
    assert set(t2.result.output) == set(task1_params.values())


def test_correct_parameter_passing_class(pipeline, runner):
    task1_params = dict(
        task1_param3="task1_param3",
        task1_param1="task1_param1",
        task1_param2="task1_param2",
    )
    task2_params = dict(task2_param1="task2_param1")

    class Task1(Task):
        @run
        def run(self, data, task1_param1, task1_param2, task1_param3):
            assert task1_param1 == "task1_param1"
            assert task1_param2 == "task1_param2"
            assert task1_param3 == "task1_param3"

            return [task1_param1, task1_param2, task1_param3]

    class Task2(Task):
        @run
        def run(self, data, task2_param1):
            assert set(data[0]) == set(task1_params.values())
            assert task2_param1 == "task2_param1"

            return data[0]

    t1 = Task1(**task1_params)
    t2 = Task2(**task2_params)

    t2(t1)
    pipeline.add(t1)
    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, order=[t1, t2])
    assert set(t2.result.output) == set(task1_params.values())


def test_branching_pipeline(pipeline, runner):
    task1_params = dict(
        task1_param3="task1_param3",
        task1_param1="task1_param1",
        task1_param2="task1_param2",
    )
    task2_params = dict(task2_param1="task2_param1")

    @task("test1", receives_input=False)
    def task1(task1_param1, task1_param2, task1_param3):
        assert task1_param1 == "task1_param1"
        assert task1_param2 == "task1_param2"
        assert task1_param3 == "task1_param3"

        return [task1_param1, task1_param2, task1_param3]

    @task("test2")
    def task2(data, task2_param1):
        assert set(data[0]) == set(task1_params.values())
        assert task2_param1 == "task2_param1"

        return data[0]

    @task("test3")
    def task3(data, task2_param1):
        assert set(data[0]) == set(task1_params.values())
        assert task2_param1 == "task2_param1"

        return data[0]

    @task("test4")
    def task4(data, task2_param1):
        assert set(data[0]) == set(task1_params.values())
        assert task2_param1 == "task2_param1"

        return data[0]

    t1 = task1(**task1_params)
    t2 = task2(**task2_params)
    t3 = task3(**task2_params)
    t4 = task4(**task2_params)

    t2(t1)
    t3(t1)
    t4(t1)
    pipeline.add(t1)
    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, edges=([t1, t2], [t1, t3], [t1, t4]))
    assert set(t2.result.output) == set(task1_params.values())


def test_merging_pipeline(pipeline, runner):
    task1_params = dict(
        task1_param3="task1_param3",
        task1_param1="task1_param1",
        task1_param2="task1_param2",
    )
    task2_params = dict(task2_param1="task2_param1")
    task3_params = dict(task3_param1="task3_param1")
    task4_params = dict(task4_param1="task4_param1", task4_param2="task4_param2")
    task5_params = dict(task5_param1="task5_param1", task5_param2="task5_param2")

    @task("test1", receives_input=False)
    def task1(task1_param1, task1_param2, task1_param3):
        assert task1_param1 == "task1_param1"
        assert task1_param2 == "task1_param2"
        assert task1_param3 == "task1_param3"

        return [task1_param1, task1_param2, task1_param3]

    @task("test2", receives_input=False)
    def task2(task2_param1):
        assert task2_param1 == "task2_param1"

        return [task2_param1]

    @task("test3")
    def task3(data, task3_param1):
        assert set(data[0]) == set(task1_params.values())
        assert set(data[1]) == set(task2_params.values())
        assert task3_param1 == "task3_param1"

        return set(data[0]) & set(data[1]) & set([task3_param1])

    @task("test4", receives_input=False)
    def task4(task4_param1, task4_param2):
        assert task4_param1 == "task4_param1"
        assert task4_param2 == "task4_param2"

        return [task4_param1, task4_param2]

    @task("test5")
    def task5(data, task5_param1, task5_param2):
        assert set(data[0]) == set(task1_params.values()) & set(
            task2_params.values()
        ) & set(task3_params.values())
        assert set(data[1]) == set(task4_params.values())
        assert task5_param1 == "task5_param1"
        assert task5_param2 == "task5_param2"

        return set(data[0]) & set(data[1]) & set([task5_param1, task5_param2])

    t1 = task1(**task1_params)
    t2 = task2(**task2_params)
    t3 = task3(**task3_params)
    t4 = task4(**task4_params)
    t5 = task5(**task5_params)

    t3(t1)(t2)
    t5(t3)(t4)
    pipeline.add(t1)
    pipeline.add(t2)
    pipeline.add(t4)
    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, edges=([t1, t3], [t2, t3], [t4, t5], [t3, t5]))
    assert set(t5.result.output) == set(task1_params.values()) & set(
        task2_params.values()
    ) & set(task3_params.values()) & set(task4_params.values()) & set(
        task5_params.values()
    )


def test_multiple_starts_at_different_levels_pipeline(pipeline, runner):
    task1_params = dict(
        task1_param3="task1_param3",
        task1_param1="task1_param1",
        task1_param2="task1_param2",
    )
    task2_params = dict(task2_param1="task2_param1")
    task3_params = dict(task3_param1="task3_param1")

    @task("test1", receives_input=False)
    def task1(task1_param1, task1_param2, task1_param3):
        assert task1_param1 == "task1_param1"
        assert task1_param2 == "task1_param2"
        assert task1_param3 == "task1_param3"

        return [task1_param1, task1_param2, task1_param3]

    @task("test2", receives_input=False)
    def task2(task2_param1):
        assert task2_param1 == "task2_param1"

        return task2_param1

    @task("test3")
    def task3(data, task3_param1):
        assert set(data[0]) == set(task1_params.values())
        assert set([data[1]]) == set(task2_params.values())
        assert task3_param1 == "task3_param1"

        return data[0]

    t1 = task1(**task1_params)
    t2 = task2(**task2_params)
    t3 = task3(**task3_params)

    t3(t1)(t2)
    pipeline.add(t1)
    pipeline.add(t2)
    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, edges=([t1, t3], [t2, t3]))
    assert set(t3.result.output) == set(task1_params.values())


def test_exception_in_task_fetched(pipeline, runner):
    @task("test1", receives_input=False)
    def task1(task1_param1, task1_param2, task1_param3):
        assert task1_param1 == "task1_param1"
        assert task1_param2 == "task1_param2"
        assert task1_param3 == "task1_param3"

    t1 = task1(
        task1_param3="task1_aram3",
        task1_param1="task1_param1",
        task1_param2="task1_param2",
    )

    pipeline.add(t1)
    runner.run(pipeline)
    with pytest.raises(AssertionError, match=r"assert 'task1_aram3' == 'task1_param3'"):
        raise t1.result.exception


def test_sequential_pipeline(runner):
    @task("simple-task")
    def simpletask(data):
        return "simple-task"

    first_task = simpletask(tag="first")
    second_task = simpletask(tag="second")
    third_task = simpletask(tag="third")
    fourth_task = simpletask(tag="fourth")
    order = [first_task, second_task, third_task, fourth_task]

    pipeline = Sequential("sequential", order)
    runner.run(pipeline)
    assert_pipeline_has_run(pipeline, order=order)
