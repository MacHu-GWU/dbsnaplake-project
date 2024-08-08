# -*- coding: utf-8 -*-

import typing as T
import json
import dataclasses

from .partition import Partition
from .s3_loc import S3Location
from .staging_to_datalake import execute_compaction
from .snapshot_to_staging import T_SNAPSHOT_DATA_FILE

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class WorkerRequest:
    """
    Worker Request.
    """

    s3uri_staging: str = dataclasses.field()
    s3uri_datalake: str = dataclasses.field()
    s3uri_partition: str = dataclasses.field()
    update_at_col: str = dataclasses.field()


@dataclasses.dataclass
class WorkerResponse:
    """
    Worker Response.
    """


def worker_lambda_handler(
    event: T.Dict[str, T.Any],
    context,
    s3_client: "S3Client",
):
    request = WorkerRequest(**event)
    s3_loc = S3Location(
        s3uri_staging=request.s3uri_staging,
        s3uri_datalake=request.s3uri_datalake,
    )
    partition = Partition.from_uri(
        s3uri=request.s3uri_partition,
        s3uri_root=request.s3uri_staging,
    )
    execute_compaction(
        partition=partition,
        s3_client=s3_client,
        s3_loc=s3_loc,
        update_at_col=request.update_at_col,
    )
