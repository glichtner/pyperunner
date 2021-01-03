import time
from pyperunner import Runner, Task, Pipeline, run


class WaitTask(Task):
    """
    Base class for tasks that just wait a given amount of seconds
    to simulate some time-consuming tasks
    """

    @run
    def run(self, data, wait, **kwargs):
        time.sleep(wait)
        return self.name


# Some class stubs as examples - in a real pipeline, these classes would have implemented run() functions.


class LoadData(WaitTask):
    pass


class ProcessData(WaitTask):
    pass


class AugmentData(WaitTask):
    pass


class Evaluate(WaitTask):
    pass


if __name__ == "__main__":
    # Create pipeline
    pipeline = Pipeline("my-pipeline")

    # Create first stream of tasks: LoadData(csv) --> ProcessData(normalize-l2)
    load_db = LoadData(
        "database",
        database={"host": "localhost", "username": "user", "password": "password"},
        wait=10,
    )
    norm_l2 = ProcessData("normalize-l2", norm="l2", axis=0, wait=1)(load_db)

    # Create second stream of tasks: LoadData(csv) --> ProcessData(normalize-l1) --> AugmentData(augment)
    load_csv = LoadData("csv", filename="data.csv", wait=1)
    norm_l1 = ProcessData("normalize-l1", norm="l1", wait=1)(load_csv)
    augment = AugmentData("augment", types=["rotate", "noise"], wait=1)(norm_l1)

    # Combine outputs of both streams (ProcessData(normalize-l2) and AugmentData(augment)),
    # additionally add output from ProcessData(normalize-l1)
    evaluate = Evaluate("both", wait=1)([norm_l2, augment])

    # Add the roots of both streams to the pipeline
    pipeline.add(load_db)
    pipeline.add(load_csv)

    # print a summary of the pipeline
    pipeline.summary()

    # Run pipeline
    runner = Runner(data_path="data/", log_path="log/")
    runner.run(pipeline)
