from contextlib import contextmanager
from typing import Any, Dict

import teradatasql
from dagster import ConfigurableResource
from dagster._utils.backoff import backoff
from packaging.version import Version
from pydantic import Field


class TeradataResource(ConfigurableResource):

    @contextmanager
    def get_connection(self):
