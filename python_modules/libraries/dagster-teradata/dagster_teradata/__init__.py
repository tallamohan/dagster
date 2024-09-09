from dagster._core.libraries import DagsterLibraryRegistry

from dagster_teradata.resource import TeradataResource as TeradataResource
from dagster_teradata.version import __version__

DagsterLibraryRegistry.register("dagster-teradata", __version__)
