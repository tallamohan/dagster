import datetime
import json
import time
from abc import ABC
from functools import cached_property
from typing import Any, Dict, List

import requests
from dagster._core.definitions.asset_key import AssetKey
from dagster._core.errors import DagsterError
from dagster._record import record
from dagster._time import get_current_datetime

from dagster_airlift.migration_state import AirflowMigrationState, DagMigrationState

from .utils import convert_to_valid_dagster_name

TERMINAL_STATES = {"success", "failed", "skipped", "up_for_retry", "up_for_reschedule"}


class AirflowAuthBackend(ABC):
    def get_session(self) -> requests.Session:
        raise NotImplementedError("This method must be implemented by subclasses.")

    def get_webserver_url(self) -> str:
        raise NotImplementedError("This method must be implemented by subclasses.")


class AirflowInstance:
    def __init__(self, auth_backend: AirflowAuthBackend, name: str) -> None:
        self.auth_backend = auth_backend
        self.name = name

    @property
    def normalized_name(self) -> str:
        return self.name.replace(" ", "_").replace("-", "_")

    def get_api_url(self) -> str:
        return f"{self.auth_backend.get_webserver_url()}/api/v1"

    def list_dags(self) -> List["DagInfo"]:
        response = self.auth_backend.get_session().get(f"{self.get_api_url()}/dags")
        if response.status_code == 200:
            dags = response.json()
            webserver_url = self.auth_backend.get_webserver_url()
            return [
                DagInfo(
                    webserver_url=webserver_url,
                    dag_id=dag["dag_id"],
                    metadata=dag,
                )
                for dag in dags["dags"]
            ]
        else:
            raise DagsterError(
                f"Failed to fetch DAGs. Status code: {response.status_code}, Message: {response.text}"
            )

    def list_variables(self) -> List[Dict[str, Any]]:
        response = self.auth_backend.get_session().get(f"{self.get_api_url()}/variables")
        if response.status_code == 200:
            return response.json()["variables"]
        else:
            raise DagsterError(
                "Failed to fetch variables. Status code: {response.status_code}, Message: {response.text}"
            )

    def get_migration_state(self) -> AirflowMigrationState:
        variables = self.list_variables()
        dag_dict = {}
        for var_dict in variables:
            if var_dict["key"].endswith("_dagster_migration_state"):
                dag_id = var_dict["key"].replace("_dagster_migration_state", "")
                migration_dict = json.loads(var_dict["value"])
                dag_dict[dag_id] = DagMigrationState.from_dict(migration_dict)
        return AirflowMigrationState(dags=dag_dict)

    def get_task_instance(self, dag_id: str, task_id: str, run_id: str) -> "TaskInstance":
        response = self.auth_backend.get_session().get(
            f"{self.get_api_url()}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}"
        )
        if response.status_code == 200:
            return TaskInstance(
                webserver_url=self.auth_backend.get_webserver_url(),
                dag_id=dag_id,
                task_id=task_id,
                run_id=run_id,
                metadata=response.json(),
            )
        else:
            raise DagsterError(
                f"Failed to fetch task instance for {dag_id}/{task_id}/{run_id}. Status code: {response.status_code}, Message: {response.text}"
            )

    def get_task_info(self, dag_id: str, task_id: str) -> "TaskInfo":
        response = self.auth_backend.get_session().get(
            f"{self.get_api_url()}/dags/{dag_id}/tasks/{task_id}"
        )
        if response.status_code == 200:
            return TaskInfo(
                webserver_url=self.auth_backend.get_webserver_url(),
                dag_id=dag_id,
                task_id=task_id,
                metadata=response.json(),
            )
        else:
            raise DagsterError(
                f"Failed to fetch task info for {dag_id}/{task_id}. Status code: {response.status_code}, Message: {response.text}"
            )

    def get_dag_source_code(self, file_token: str) -> str:
        response = self.auth_backend.get_session().get(
            f"{self.get_api_url()}/dagSources/{file_token}"
        )
        if response.status_code == 200:
            return response.text
        else:
            raise DagsterError(
                f"Failed to fetch source code. Status code: {response.status_code}, Message: {response.text}"
            )

    @staticmethod
    def airflow_str_from_datetime(dt: datetime.datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def get_dag_runs(
        self, dag_id: str, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> List["DagRun"]:
        response = self.auth_backend.get_session().get(
            f"{self.get_api_url()}/dags/{dag_id}/dagRuns",
            params={
                "updated_at_gte": self.airflow_str_from_datetime(start_date),
                "updated_at_lte": self.airflow_str_from_datetime(end_date),
                "state": ["success"],
            },
        )
        if response.status_code == 200:
            webserver_url = self.auth_backend.get_webserver_url()
            return [
                DagRun(
                    webserver_url=webserver_url,
                    dag_id=dag_id,
                    run_id=dag_run["dag_run_id"],
                    metadata=dag_run,
                )
                for dag_run in response.json()["dag_runs"]
            ]
        else:
            raise DagsterError(
                f"Failed to fetch dag runs for {dag_id}. Status code: {response.status_code}, Message: {response.text}"
            )

    def trigger_dag(self, dag_id: str) -> str:
        response = self.auth_backend.get_session().post(
            f"{self.get_api_url()}/dags/{dag_id}/dagRuns",
            json={},
        )
        if response.status_code != 200:
            raise DagsterError(
                f"Failed to launch run for {dag_id}. Status code: {response.status_code}, Message: {response.text}"
            )
        return response.json()["dag_run_id"]

    def get_dag_run(self, dag_id: str, run_id: str) -> "DagRun":
        response = self.auth_backend.get_session().get(
            f"{self.get_api_url()}/dags/{dag_id}/dagRuns/{run_id}"
        )
        if response.status_code != 200:
            raise DagsterError(
                f"Failed to fetch dag run for {dag_id}/{run_id}. Status code: {response.status_code}, Message: {response.text}"
            )
        return DagRun(
            webserver_url=self.auth_backend.get_webserver_url(),
            dag_id=dag_id,
            run_id=run_id,
            metadata=response.json(),
        )

    def wait_for_run_completion(self, dag_id: str, run_id: str, timeout: int = 30) -> None:
        start_time = get_current_datetime()
        while get_current_datetime() - start_time < datetime.timedelta(seconds=timeout):
            dag_run = self.get_dag_run(dag_id, run_id)
            if dag_run.finished:
                return
            time.sleep(1)
        raise DagsterError(f"Timed out waiting for airflow run {run_id} to finish.")

    @staticmethod
    def timestamp_from_airflow_date(airflow_date: str) -> float:
        try:
            return datetime.datetime.strptime(airflow_date, "%Y-%m-%dT%H:%M:%S+00:00").timestamp()
        except ValueError:
            return datetime.datetime.strptime(
                airflow_date, "%Y-%m-%dT%H:%M:%S.%f+00:00"
            ).timestamp()

    @staticmethod
    def airflow_date_from_datetime(datetime: datetime.datetime) -> str:
        return datetime.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def delete_run(self, dag_id: str, run_id: str) -> None:
        response = self.auth_backend.get_session().delete(
            f"{self.get_api_url()}/dags/{dag_id}/dagRuns/{run_id}"
        )
        if response.status_code != 204:
            raise DagsterError(
                f"Failed to delete run for {dag_id}/{run_id}. Status code: {response.status_code}, Message: {response.text}"
            )
        return None


@record
class DagInfo:
    webserver_url: str
    dag_id: str
    metadata: Dict[str, Any]

    @property
    def url(self) -> str:
        return f"{self.webserver_url}/dags/{self.dag_id}"

    @cached_property
    def dagster_safe_dag_id(self) -> str:
        """Name based on the dag_id that is safe to use in dagster."""
        return convert_to_valid_dagster_name(self.dag_id)

    @property
    def dag_asset_key(self) -> AssetKey:
        # Conventional asset key representing a successful run of an airfow dag.
        return AssetKey(["airflow_instance", "dag", self.dagster_safe_dag_id])

    @property
    def file_token(self) -> str:
        return self.metadata["file_token"]


@record
class TaskInfo:
    webserver_url: str
    dag_id: str
    task_id: str
    metadata: Dict[str, Any]

    @property
    def dag_url(self) -> str:
        return f"{self.webserver_url}/dags/{self.dag_id}"


@record
class TaskInstance:
    webserver_url: str
    dag_id: str
    task_id: str
    run_id: str
    metadata: Dict[str, Any]

    @property
    def note(self) -> str:
        return self.metadata.get("note") or ""

    @property
    def details_url(self) -> str:
        return f"{self.webserver_url}/dags/{self.dag_id}/grid?dag_run_id={self.run_id}&task_id={self.task_id}"

    @property
    def log_url(self) -> str:
        return f"{self.details_url}&tab=logs"

    @property
    def start_date(self) -> float:
        return AirflowInstance.timestamp_from_airflow_date(self.metadata["start_date"])

    @property
    def end_date(self) -> float:
        return AirflowInstance.timestamp_from_airflow_date(self.metadata["end_date"])


@record
class DagRun:
    webserver_url: str
    dag_id: str
    run_id: str
    metadata: Dict[str, Any]

    @property
    def note(self) -> str:
        return self.metadata.get("note") or ""

    @property
    def url(self) -> str:
        return f"{self.webserver_url}/dags/{self.dag_id}/grid?dag_run_id={self.run_id}&tab=details"

    @property
    def success(self) -> bool:
        return self.metadata["state"] == "success"

    @property
    def finished(self) -> bool:
        return self.metadata["state"] in TERMINAL_STATES

    @property
    def run_type(self) -> str:
        return self.metadata["run_type"]

    @property
    def config(self) -> Dict[str, Any]:
        return self.metadata["conf"]

    @property
    def start_date(self) -> float:
        return AirflowInstance.timestamp_from_airflow_date(self.metadata["start_date"])

    @property
    def start_datetime(self) -> datetime.datetime:
        return datetime.datetime.strptime(self.metadata["start_date"], "%Y-%m-%dT%H:%M:%S+00:00")

    @property
    def end_date(self) -> float:
        return AirflowInstance.timestamp_from_airflow_date(self.metadata["end_date"])

    @property
    def end_datetime(self) -> datetime.datetime:
        return datetime.datetime.strptime(self.metadata["end_date"], "%Y-%m-%dT%H:%M:%S+00:00")
