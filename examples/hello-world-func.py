from pyperunner import Pipeline, Runner, task


@task("Hello", receives_input=False)
def hello():
    """Example task that returns "Hello" """
    print("in hello()")
    return "Hello"


@task("World")
def world(data):
    """
    Example task that appends "World" to the input data.
    """
    hello = data["Hello()"]
    print("in world()")
    return f"{hello} world"


# instantiate and connect tasks
hello = hello()
world = world()(hello)

# create pipeline and set root element
pipeline = Pipeline("hello-world-example", [hello])

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
