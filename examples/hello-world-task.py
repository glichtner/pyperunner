from pyperunner import Runner, Pipeline, task


@task("Hello", receives_input=False)
def hello():
    return "Hello"


@task("World")
def world(data):
    return f"{data} world"


# instantiate a new pipeline
pipeline = Pipeline("hello-world-example")

# instantiate tasks
hello = hello()
world = world()

# connect tasks with pipeline
pipeline(hello(world))

runner = Runner(data_path="data/", log_path="log/")
runner.run(pipeline)
