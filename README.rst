PypeRunner
##########
.. start-badges

.. image:: https://readthedocs.org/projects/pyperunner/badge/?version=latest
    :target: https://pyperunner.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://badge.fury.io/py/pyperunner.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/pyperunner

.. image:: https://img.shields.io/pypi/pyversions/pyperunner.svg
    :alt: Supported Python Versions
    :target: https://pypi.org/project/pyperunner/

.. image:: https://pepy.tech/badge/pyperunner
    :alt: Downloads
    :target: https://pepy.tech/project/pyperunner/

.. end-badges

Yet another ETL pipeline runner for pure python, using multiprocessing and directed acyclic graphs.

Features
========

- Parallel processing of steps
- Caching of previously run steps to speed up processing
- Re-run of steps (and all subsequent steps) when parameters are changed
- Easy creation of pipelines using functional API chaining (see below)
- Save & read pipelines to/from yaml
- Graphical output of pipeline run

Installation
============

Install from pip via

.. code-block:: bash

    pip install pyperunner

Or from source via

.. code-block:: bash

    git clone https://github.com/glichtner/pyperunner.git
    cd pyperunner
    python setup.py install


Quickstart
==========

Pyperunner has three basic components:

* **Task**: Definition of the work to do (Python classes or functions)
* **Pipeline**: A collection of tasks that are connected in a directed fashion
* **Runner**: The executor of a pipeline

Hello world example
-------------------

.. code-block:: python

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


Running this script outputs the following:

.. code-block:: console

    ~/pyperunner/examples$ python hello-world.py

    2021-01-02 18:58:20 INFO     Process-1    Hello()    Starting
    2021-01-02 18:58:20 INFO     Process-1    Hello()    Finished: Status.SUCCESS
    2021-01-02 18:58:20 INFO     Process-2    World()    Starting
    2021-01-02 18:58:20 INFO     Process-2    World()    Finished: Status.SUCCESS

Note that if you re-run the script, pyperunner will detect that the current configuration has already run and will use cached outputs:

.. code-block:: console

    ~/pyperunner/examples$ python hello-world.py

    2021-01-02 19:01:28 INFO     Process-1    Hello()    Starting
    2021-01-02 19:01:28 INFO     Process-1    Hello()    Loading output from disk, skipping processing
    2021-01-02 19:01:28 INFO     Process-1    Hello()    Finished: Status.SUCCESS
    2021-01-02 19:01:28 INFO     Process-2    World()    Starting
    2021-01-02 19:01:28 INFO     Process-2    World()    Loading output from disk, skipping processing
    2021-01-02 19:01:28 INFO     Process-2    World()    Finished: Status.SUCCESS

At each run, the pipeline is automatically stored in a yaml file in the log path to ensure reproducibility:

.. code-block:: yaml

    pipeline:
      name: hello-world-example
    tasks:
      Hello():
        hash: 22179f3afd85ab64dd32c63bc21a9eb4
        module: __main__
        name: Hello
        params: {}
        parents: []
        tag: ''
      World():
        hash: f7d904856f2aa4fda20e05521298397f
        module: __main__
        name: World
        params: {}
        parents:
        - Hello()
        tag: ''

Additionally, a graphical representation of the run is saved in the log path:

.. image:: examples/hello-world-status.png
   :width: 20%
   :alt: Hello World pipeline status
   :align: center

Documentation
=============

The `API Reference <http://pyperunner.readthedocs.io>`_ provides API-level documentation.
