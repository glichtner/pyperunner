from pyperunner import Runner, Pipeline, Task, run


class Hello(Task):
    @run
    def run(self, data):
        return "Hello"


class World(Task):
    @run
    def run(self, data):
        return f"{data} world"


# instantiate a new pipeline
pipeline = Pipeline("hello-world-example")

# instantiate tasks
hello = Hello()
world = World()

# connect tasks with pipeline
pipeline(hello(world))

runner = Runner(data_path="data/", log_path="log/")
runner.run(pipeline)
