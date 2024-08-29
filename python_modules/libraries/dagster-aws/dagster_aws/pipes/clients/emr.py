from typing import TYPE_CHECKING, Optional

import boto3
from dagster._annotations import experimental
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.pipes.client import PipesContextInjector, PipesMessageReader
from mypy_boto3_emr.type_defs import (
    DescribeClusterOutputTypeDef,
    RunJobFlowInputRequestTypeDef,
    RunJobFlowOutputTypeDef,
)

from dagster_aws.pipes.clients.base import PipesBaseClient, class_docstring, run_docstring
from dagster_aws.pipes.message_readers import PipesCloudWatchMessageReader

if TYPE_CHECKING:
    from mypy_boto3_emr.literals import ClusterStateType


AWS_SERVICE_NAME = "EMR"


@experimental
@class_docstring(AWS_SERVICE_NAME)
class PipesEMRClient(
    PipesBaseClient[
        RunJobFlowInputRequestTypeDef, RunJobFlowOutputTypeDef, DescribeClusterOutputTypeDef
    ]
):
    AWS_SERVICE_NAME = AWS_SERVICE_NAME

    def __init__(
        self,
        client=None,
        context_injector: Optional[PipesContextInjector] = None,
        message_reader: Optional[PipesMessageReader] = None,
        forward_termination: bool = True,
    ):
        super().__init__(
            client=client or boto3.client("emr"),
            context_injector=context_injector,
            message_reader=message_reader,
            forward_termination=forward_termination,
        )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    # is there a better way to do this?
    # is it possible to automatically inject the class attributes into the method docs?
    run = run_docstring(AWS_SERVICE_NAME, "run_job_flow")(PipesBaseClient.run)

    def _start(
        self, context: OpExecutionContext, params: "RunJobFlowInputRequestTypeDef"
    ) -> "RunJobFlowOutputTypeDef":
        response = self._client.run_job_flow(**params)
        cluster_id = response["JobFlowId"]
        context.log.info(
            f"[pipes] {self.AWS_SERVICE_NAME} job started with cluster id {cluster_id}"
        )
        return response

    def _wait_for_completion(
        self, context: OpExecutionContext, start_response: RunJobFlowOutputTypeDef
    ) -> "DescribeClusterOutputTypeDef":
        cluster_id = start_response["JobFlowId"]
        self._client.get_waiter("cluster_running").wait(ClusterId=cluster_id)
        context.log.info(f"[pipes] {self.AWS_SERVICE_NAME} job {cluster_id} running")
        # now wait for the job to complete
        self._client.get_waiter("cluster_terminated").wait(ClusterId=cluster_id)

        response = self._client.describe_cluster(ClusterId=cluster_id)

        state: ClusterStateType = response["Cluster"]["Status"]["State"]

        context.log.info(
            f"[pipes] {self.AWS_SERVICE_NAME} job {cluster_id} completed with state: {state}"
        )

        if state == "FAILED":
            context.log.error(f"[pipes] {self.AWS_SERVICE_NAME} job {cluster_id} failed")
            raise Exception(f"[pipes] {self.AWS_SERVICE_NAME} job {cluster_id} failed")

        return response

    def _read_messages(self, context: OpExecutionContext, response: DescribeClusterOutputTypeDef):
        if isinstance(self._message_reader, PipesCloudWatchMessageReader):
            # we can get cloudwatch logs from the known log group
            log_group, log_stream = response["Cluster"]["LogUri"].split(":", 1)  # pyright: ignore (reportTypedDictNotRequiredAccess)
            context.log.info(f"[pipes] Reading logs from {log_group}/{log_stream}")
            self._message_reader.consume_cloudwatch_logs(
                log_group,
                log_stream,
                start_time=int(
                    response["Cluster"]["Status"]["Timeline"]["CreationDateTime"].timestamp() * 1000  # pyright: ignore (reportTypedDictNotRequiredAccess)
                ),
            )

    def _terminate(self, context: OpExecutionContext, start_response: RunJobFlowOutputTypeDef):
        cluster_id = start_response["JobFlowId"]
        context.log.info(f"[pipes] Terminating {self.AWS_SERVICE_NAME} job {cluster_id}")
        self._client.terminate_job_flows(JobFlowIds=[cluster_id])
