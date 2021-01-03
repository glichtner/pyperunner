from pyperunner import Runner, Sequential, task


@task("Hello", receives_input=False)
def hello():
    print("in hello()")
    return "Hello"


@task("World")
def world(data):
    print("in world()")
    return f"{data} world"


# create a sequential pipeline and supply whole pipeline as list in constructor
pipeline = Sequential("hello-world-example", [hello(), world()])
pipeline.summary()

# run pipeline
runner = Runner(data_path="data/", log_path="log/")
runner.run(pipeline, force_reload=True)
