import os
import time
import sys
sys.path.append(os.path.relpath('..'))
from pyperunner import __version__

extensions = [
    'sphinx.ext.autodoc',
    'scanpydoc.elegant_typehints',
    'sphinx.ext.autosummary',
    'sphinx.ext.coverage',
    'sphinx.ext.doctest',
    'sphinx.ext.extlinks',
    'sphinx.ext.ifconfig',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
    'sphinx.ext.todo',
    'sphinx.ext.viewcode',
    'sphinx.ext.graphviz',
]

autodoc_mock_imports = ['multiprocessing']

if os.getenv('SPELLCHECK'):
    extensions += 'sphinxcontrib.spelling',
    spelling_show_suggestions = True
    spelling_lang = 'en_US'

source_suffix = '.rst'
master_doc = 'index'
project = 'pyperunner'
author = 'Gregor Lichtner'
year = f'2021-{time.strftime("%Y")}'
copyright = '{0}, {1}'.format(year, author)
version = release = __version__

pygments_style = 'trac'
templates_path = ['.']
extlinks = {
    'issue': ('https://github.com/glichtner/pyperunner/%s', '#'),
    'pr': ('https://github.com/glichtner/pyperunner/pull/%s', 'PR #'),
}
# on_rtd is whether we are on readthedocs.org
on_rtd = os.environ.get('READTHEDOCS', None) == 'True'

if not on_rtd:  # only set the theme if we're building docs locally
    html_theme = 'sphinx_rtd_theme'

html_use_smartypants = True
html_last_updated_fmt = '%b %d, %Y'
html_split_index = False
html_sidebars = {
   '**': ['searchbox.html', 'globaltoc.html', 'sourcelink.html'],
}
html_short_title = '%s-%s' % (project, version)

napoleon_use_ivar = True
napoleon_use_rtype = True
napoleon_use_param = True

# workaround for https://github.com/sphinx-doc/sphinx/issues/7493
# see https://icb-scanpydoc.readthedocs-hosted.com/en/latest/scanpydoc.elegant_typehints.html
qualname_overrides = {
    "pyperunner.pipeline.Pipeline": "pyperunner.Pipeline",
    "pyperunner.pipeline.Sequential": "pyperunner.Sequential",
    "pyperunner.task.Task": "pyperunner.Task",
    "pyperunner.task.Task.Status": "pyperunner.Task.Status",
    "pyperunner.task.Task.TaskResult": "pyperunner.Task.TaskResult",
    "pyperunner.runner.multiprocess.Runner": "pyperunner.Runner",
    "pyperunner.decorator.run": "pyperunner.run",
    "pyperunner.decorator.task": "pyperunner.task",
    "pyperunner.util.PipelineResult": "pyperunner.PipelineResult",
    "pyperunner.util.TaskResult": "pyperunner.TaskResult",
    "pyperunner.task.Task.TaskResult": "pyperunner.Task.TaskResult"
}
annotate_defaults = False