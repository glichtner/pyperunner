from pyperunner import Runner, Pipeline, Task, run


class Hello(Task):
    @run
    def run(self, data):
        return "Hello"


class World(Task):
    @run
    def run(self, data):
        return f"{data} world"


# instantiate and connect tasks
hello = Hello()
world = World()(hello)

# create pipeline and set root element
pipeline = Pipeline("hello-world-example", [hello])

# run pipeline
runner = Runner(data_path="data/", log_path="log/")
runner.run(pipeline)
