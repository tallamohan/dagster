from contextlib import contextmanager
from typing import Any, Dict, Optional

import teradatasql
from dagster import ConfigurableResource
from dagster._utils.backoff import backoff
from packaging.version import Version
from pydantic import Field


class TeradataResource(ConfigurableResource):

    host: str = Field(default=None, description="Teradata Database Hostname")

    user: str = Field(description="User login name.")

    password: Optional[str] = Field(default=None, description="User password.")

    database: Optional[str] = Field(
        default=None,
        description=("Name of the default database to use."),
    )

    connection_config: Dict[str, Any] = Field(
        default={},
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @contextmanager
    def get_connection(self):
        config = self.connection_config
        teradata_conn = teradatasql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
        yield teradata_conn