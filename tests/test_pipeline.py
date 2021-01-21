import pytest


def assert_pipeline_has_run(runner, pipeline, order):
    node = pipeline.root

    for task in order:
        assert len(node.children) == 1
        child = node.children[0]
        assert child is task
        node = child

        assert task.output_exists()


def test_single_task_pipeline(task_simple_no_input, pipeline, runner):
    ts1 = task_simple_no_input()
    pipeline.add(ts1)
    runner.run(pipeline)
    assert_pipeline_has_run(runner, pipeline, order=[ts1])


def test_second_task_accepts_no_parameter(task_simple_no_input, pipeline, runner):
    ts1 = task_simple_no_input("tag1")
    ts2 = task_simple_no_input("tag2")
    pipeline.add(ts1)
    ts2(ts1)
    runner.run(pipeline)
    assert_pipeline_has_run(runner, pipeline, order=[ts1, ts2])


def test_task_with_input_as_first_task(task_simple, pipeline, runner):
    ts1 = task_simple()
    pipeline.add(ts1)
    runner.run(pipeline)
    assert_pipeline_has_run(runner, pipeline, order=[ts1])


def test_concat_tasks_first_then_add_to_pipeline(task_simple, pipeline, runner):
    ts1 = task_simple("tag1")
    ts2 = task_simple("tag2")
    ts3 = task_simple("tag3")
    ts3(ts2)
    ts2(ts1)
    pipeline.add(ts1)

    runner.run(pipeline)
    assert_pipeline_has_run(runner, pipeline, order=[ts1, ts2, ts3])


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
    assert_pipeline_has_run(runner, pipeline, order=[ts1, ts2, ts3])


def test_multiple_tasks_with_same_name_error(task_simple, pipeline, runner):
    ts_1 = task_simple()
    ts_2 = task_simple()

    pipeline.add(ts_1)
    with pytest.raises(ValueError, match="Node names must be unique"):
        ts_1(ts_2)


def test_correct_parameter_passing(pipeline, runner):
    # TODO: implement me
    pass


def test_branching_pipeline(pipeline, runner):
    # TODO: implement me --> also ensure correct data passing
    pass


def test_merging_pipeline(pipeline, runner):
    # TODO: implement me --> also ensure correct data passing
    pass


def test_multiple_starts_at_different_levels_pipeline(pipeline, runner):
    # TODO: implement me --> also ensure correct data passing
    pass


def test_exception_in_task_fetched():
    # TODO: implement me
    pass
