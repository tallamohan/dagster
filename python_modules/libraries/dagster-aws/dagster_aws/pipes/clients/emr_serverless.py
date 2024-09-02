import time
from typing import TYPE_CHECKING, Any, Dict, Optional

import boto3
import dagster._check as check
from dagster import PipesClient
from dagster._annotations import public
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.pipes.client import (
    PipesClientCompletedInvocation,
    PipesContextInjector,
    PipesMessageReader,
)
from dagster._core.pipes.utils import PipesEnvContextInjector, open_pipes_session

from dagster_aws.pipes.message_readers import PipesCloudWatchMessageReader

if TYPE_CHECKING:
    from mypy_boto3_emr_serverless.client import EMRServerlessClient
    from mypy_boto3_emr_serverless.type_defs import (
        GetJobRunResponseTypeDef,
        JobRunStateType,
        StartJobRunRequestRequestTypeDef,
        StartJobRunResponseTypeDef,
    )

AWS_SERVICE_NAME = "EMR Serverless"
AWS_SERVICE_NAME_LOWER = AWS_SERVICE_NAME.replace(" ", "").lower()
AWS_SERVICE_NAME_LOWER_KEBAB = AWS_SERVICE_NAME_LOWER.replace("_", "-")
BOTO3_START_METHOD = "run_job_flow"


class PipesEMRClient(
    PipesClient,
    TreatAsResourceParam,
):
    f"""A pipes client for running workloads on AWS {AWS_SERVICE_NAME}.

    Args:
        client (Optional[boto3.client]): The boto3 {AWS_SERVICE_NAME} client used to interact with AWS {AWS_SERVICE_NAME}.
        context_injector (Optional[PipesContextInjector]): A context injector to use to inject
            context into AWS {AWS_SERVICE_NAME} workload. Defaults to :py:class:`PipesEnvContextInjector`.
        message_reader (Optional[PipesMessageReader]): A message reader to use to read messages
            from the {AWS_SERVICE_NAME} workload. Defaults to :py:class:`PipesCloudWatchMessageReader`.
        forward_termination (bool): Whether to cancel the {AWS_SERVICE_NAME} workload if the Dagster process receives a termination signal.
    """

    AWS_SERVICE_NAME = AWS_SERVICE_NAME

    def __init__(
        self,
        client=None,
        context_injector: Optional[PipesContextInjector] = None,
        message_reader: Optional[PipesMessageReader] = None,
        forward_termination: bool = True,
    ):
        self._client = client or boto3.client(AWS_SERVICE_NAME_LOWER_KEBAB)
        self._context_injector = context_injector or PipesEnvContextInjector()
        self._message_reader = message_reader or PipesCloudWatchMessageReader()
        self.forward_termination = check.bool_param(forward_termination, "forward_termination")

    @property
    def client(self) -> EMRServerlessClient:
        return self._client

    @property
    def context_injector(self) -> PipesContextInjector:
        return self._context_injector

    @property
    def message_reader(self) -> PipesMessageReader:
        return self._message_reader

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @public
    def run(
        self,
        *,
        context: OpExecutionContext,
        start_job_run_params: StartJobRunRequestRequestTypeDef,
        extras: Optional[Dict[str, Any]] = None,
    ) -> PipesClientCompletedInvocation:
        f"""Run a workload on AWS {AWS_SERVICE_NAME}, enriched with the pipes protocol.

            Args:
                context (OpExecutionContext): The context of the currently executing Dagster op or asset.
                params (dict): Parameters for the ``{BOTO3_START_METHOD}`` boto3 {AWS_SERVICE_NAME} client call.
                    See `Boto3 API Documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/{AWS_SERVICE_NAME_LOWER}/client/{AWS_SERVICE_NAME_LOWER}.html#{AWS_SERVICE_NAME_LOWER_KEBAB}>`_
                extras (Optional[Dict[str, Any]]): Additional information to pass to the Pipes session in the external process.

            Returns:
                PipesClientCompletedInvocation: Wrapper containing results reported by the external
                process.
            """
        with open_pipes_session(
            context=context,
            message_reader=self.message_reader,
            context_injector=self.context_injector,
            extras=extras,
        ) as session:
            start_response = self._start(context, start_job_run_params)
            try:
                completion_response = self._wait_for_completion(context, start_response)
                context.log.info(f"[pipes] {self.AWS_SERVICE_NAME} workload is complete!")
                self._read_messages(context, completion_response)
                return PipesClientCompletedInvocation(session)

            except DagsterExecutionInterruptedError:
                if self.forward_termination:
                    context.log.warning(
                        f"[pipes] Dagster process interrupted! Will terminate external {self.AWS_SERVICE_NAME} workload."
                    )
                    self._terminate(context, start_response)
                raise

    def _start(
        self, context: OpExecutionContext, params: StartJobRunRequestRequestTypeDef
    ) -> StartJobRunResponseTypeDef:
        response = self._client.start_job_run(**params)
        job_run_id = response["jobRunId"]
        context.log.info(
            f"[pipes] {self.AWS_SERVICE_NAME} job started with job_run_id {job_run_id}"
        )
        return response

    def _wait_for_completion(
        self, context: OpExecutionContext, start_response: StartJobRunResponseTypeDef
    ) -> GetJobRunResponseTypeDef:
        job_run_id = start_response["jobRunId"]

        while response := self._client.get_job_run(JobRunId=job_run_id):
            state: JobRunStateType = response["state"]

            if state in ["FAILED", "CANCELLED", "CANCELLING"]:
                context.log.error(
                    f"[pipes] {self.AWS_SERVICE_NAME} job {job_run_id} terminated with state: {state}"
                )
                raise RuntimeError(f"[pipes] {self.AWS_SERVICE_NAME} job {job_run_id} failed")

            elif state == "SUCCEEDED":
                context.log.info(
                    f"[pipes] {self.AWS_SERVICE_NAME} job {job_run_id} completed with state: {state}"
                )
                return response

            time.sleep(10)

    def _read_messages(self, context: OpExecutionContext, response: GetJobRunResponseTypeDef):
        if isinstance(self.message_reader, PipesCloudWatchMessageReader):
            # we can get cloudwatch logs from the known log group
            monitoring_configuration = response["jobRun"]["configurationOverrides"][
                "monitoringConfiguration"
            ]
            if not monitoring_configuration["cloudWatchLoggingConfiguration"]["enabled"]:
                context.log.warning(
                    f"[pipes] Recieved {self.message_reader}, but CloudWatch logging is not enabled for {self.AWS_SERVICE_NAME} job. Dagster won't be able to receive logs and messages from the job."
                )
                return

            log_group = monitoring_configuration["cloudWatchLoggingConfiguration"]["logGroupName"]
            log_stream_prefix = monitoring_configuration["cloudWatchLoggingConfiguration"][
                "logStreamNamePrefix"
            ]

            context.log.info(f"[pipes] Reading logs from {log_group}/{log_stream_prefix}")
            self.message_reader.consume_cloudwatch_logs(
                log_group,
                log_stream_prefix,
                start_time=int(response["jobRun"]["attemptCreatedAt"].timestamp() * 1000),
            )

        else:
            context.log.warning(
                f"[pipes] {self.message_reader} is not supported for {self.AWS_SERVICE_NAME}. Dagster won't be able to receive logs and messages from the job."
            )

    def _terminate(self, context: OpExecutionContext, start_response: StartJobRunResponseTypeDef):
        job_run_id = start_response["jobRunId"]
        context.log.info(f"[pipes] Terminating {self.AWS_SERVICE_NAME} job run {job_run_id}")
        self._client.cancel_job_run(jobRunId=job_run_id)
