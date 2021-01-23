PypeRunner Components
#####################

PypeRunner has three main building blocks:

* :py:class:`~pyperunner.Task` define the work units
* :py:class:`~pyperunner.Pipeline` defines the order in which the tasks are executed
* :py:class:`~pyperunner.Runner` orchestrates the execution of tasks in a pipeline

Task
====
A task is the

Task Name
---------

Task Parameter
--------------

Task Hash
---------


Memory consumption
------------------
  - RAM problem


Pipeline
========

Pipeline

Runner
======



Define Tasks
############
Tasks can be defined either from functions or by subclassing the :py:class:`~pyperunner.Task` class,
Which way to use depends on the complexity and organization of the task's code: For a simple
workflow a function usually suffices and is easier readable, while for complex workflows with
multiple function calls and the requirement to keep a state, a class might be the better choice.

Function as task
==================
To use a user-defined function as a task, simply tag it with the :py:func:`~pyperunner.task` decorator:

.. code-block:: python

    from pyperunner import task

    @task("MyTask")
    def my_function(data):
        result = do_something(data)
        return result

    # note that creating the task is performed by calling the function
    # but *without* the data argument.
    my_task = my_function()

The :py:func:`~pyperunner.task` decorator has a required positional parameter `name`, which is used as the name of the
task. It is required that each task in pipeline has a unique name.

The function that is decorated by the :py:func:`~pyperunner.task` decorator needs to either accept a named parameter
`data` or the task() decorator needs to be supplied a `receives_input=False` parameter.

.. code-block:: python

    # Create a task named "SimpleTask" - note that the function accepts
    # the `data` keyword parameter
    @task('SimpleTaskWithInput')
    def simpletask_with_input(data):
        return 'simple-task'

    # If the function should not accept data (i.e. be a starting task of the pipeline),
    # the `receives_input=False` parameter must be supplied to the task() decorator.
    @task('SimpleTaskWithoutInput', receives_input=False)
    def simpletask_without_input():
        return 'simple-task'

    # The following definition will raise an AttributeError
    @task('SimpleTaskError')
    def simpletask_without_input_error():
        return 'simple-task'
    # raises AttributeError: To receive input data, the function must accept
    # the named parameter "data"


You can add additional parameters to the function definition. These then need to be supplied during task creation time:

.. code-block:: python

    @task("MyTaskWithParameters")
    def my_function(data, reduce, n_iterations=10):
        result = data
        for i in range(n_iterations):
            result = do_something(result, reduce=reduce)

        return result

    # The task is created by calling the function but always *without*
    # the `data` argument.
    my_task = my_function(reduce=True, n_iterations=20)

    # Parameters with default values may be skipped during task creation
    my_task = my_function(reduce=True)


Class as task
==================
  - task via class b(Task) (mit allen parametern)

Combine Tasks to Pipeline
#########################
  - output path und log path
  - plot graph
  - graph summary (ascii)

Standard Pipeline
==================

Sequential Pipeline
========================
  - sequential pipeline

Save & load pipelines
========================
  - pipeline to yml
  - pipeline from yml

Run Pipeline
==================
  - logging (colored to cmd, and to log file)

Access pipeline results
#######################
  - pipeline results

pipeline.results()
==================


PipelineResults
==================

Filesystem
----------





Reproducibility
###############
ref save and load
  - environment (pipeline run)

Result Caching
==============
  - caching strategy
    - hashs (task)

