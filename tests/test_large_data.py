import pytest


@pytest.mark.slow
def test_large_dataset(task_large_data, task_simple, pipeline, runner):
    ts1 = task_large_data("tag1", n_bytes=2 * (1024 ** 3))
    ts2 = task_simple("tag1")
    pipeline.add(ts1)
    ts2(ts1)
    runner.run(pipeline)
    # assert_pipeline_has_run(runner, pipeline, order=[ts1])
    # TODO: implement me
