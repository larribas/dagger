"""
Run DAGs or Tasks taking their inputs from files and storing their outputs into files.

It defines a Command-Line Interface through which users can specify all input/output locations,
and optionally select a specific task to invoke.

It runs all tasks/DAGs using the "local" runtime.
"""

from dagger.runtime.cli.cli import invoke  # noqa
from dagger.runtime.cli.locations import PARTITION_MANIFEST_FILENAME  # noqa
