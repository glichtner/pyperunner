from pyperunner import Runner, Sequential, Task, run


class Hello(Task):
    @run
    def run(self, data):
        return "Hello"


class World(Task):
    @run
    def run(self, data):
        return f"{data} world"


# create a sequential pipeline and supply whole pipeline as list in constructor
pipeline = Sequential("hello-world-example", [Hello(), World()])

# print a summary of the pipeline
pipeline.summary()

# run pipeline
runner = Runner(data_path="data/", log_path="log/")
runner.run(pipeline)

# get pipeline results object from the pipeline that was just run
results = runner.results()

# show the results
for task_name in results:
    print(f"Output of task '{task_name}' was '{results[task_name]}'")
