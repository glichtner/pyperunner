import time
from pyperunner import Runner, Task, Pipeline, run


class WaitTask(Task):
    @run
    def run(self, data, wait, **kwargs):
        time.sleep(wait)
        return self.name


class LoadDataTask(WaitTask):
    pass


class ProcessDataTask(WaitTask):
    pass


class EvaluateDataTask(WaitTask):
    pass


if __name__ == "__main__":
    p = Pipeline("my-pipeline")
    d1 = LoadDataTask(
        "database",
        database={"host": "localhost", "username": "user", "password": "password"},
        wait=10,
    )
    d2 = LoadDataTask("csv", filename="data.csv", wait=1)

    p1 = ProcessDataTask("normalize-l2", norm="l2", axis=0, wait=1)
    p2 = ProcessDataTask("normalize-l1", norm="l1", wait=1)
    p3 = ProcessDataTask("augment", param="params3", wait=1)
    e = EvaluateDataTask("both", wait=1)

    # stream 1
    p(d1(p1(e)))

    # stream 2
    p(d2(p2(p3(e))))
    p2(e)

    runner = Runner(data_path="data/", log_path="log/")
    runner.run(p, force_reload=False)
