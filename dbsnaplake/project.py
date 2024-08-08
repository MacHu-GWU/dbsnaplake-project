# -*- coding: utf-8 -*-

import typing as T
import dataclasses
from datetime import datetime
from functools import cached_property
import polars as pl

from .typehint import T_RECORD, T_DF_SCHEMA, T_OPTIONAL_KWARGS
from .logger import dummy_logger
from .s3_loc import S3Location
from .partition import Partition, get_partitions
from .snapshot_to_staging import (
    SnapshotDataFile,
    T_SNAPSHOT_DATA_FILE,
    StagingDataFile,
    DerivedColumn,
    process_snapshot_data_file,
)
from .staging_to_datalake import (
    DatalakeDataFile,
    execute_compaction,
)

if T.TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class BaseProject:
    s3_client: "S3Client" = dataclasses.field()
    s3uri_staging: str = dataclasses.field()
    s3uri_datalake: str = dataclasses.field()
    extract_record_id: DerivedColumn = dataclasses.field()
    extract_create_time: DerivedColumn = dataclasses.field()
    extract_update_time: DerivedColumn = dataclasses.field()
    extract_partition_keys: T.List[DerivedColumn] = dataclasses.field()
    df_schema: T_DF_SCHEMA = dataclasses.field()

    @cached_property
    def s3_loc(self) -> S3Location:
        return S3Location(
            s3uri_staging=self.s3uri_staging,
            s3uri_datalake=self.s3uri_datalake,
        )

    def process_snapshot_data_file(
        self,
        snapshot_data_file: T_SNAPSHOT_DATA_FILE,
        n_lines: T.Optional[int] = None,
        polars_dataframe_kwargs: T_OPTIONAL_KWARGS = None,
        polars_write_parquet_kwargs: T_OPTIONAL_KWARGS = None,
        s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
        logger=dummy_logger,
    ):
        process_snapshot_data_file(
            snapshot_data_file=snapshot_data_file,
            s3_client=self.s3_client,
            schema=self.df_schema,
            extract_record_id=self.extract_record_id,
            extract_create_time=self.extract_create_time,
            extract_update_time=self.extract_update_time,
            extract_partition_keys=self.extract_partition_keys,
            s3_loc=self.s3_loc,
            n_lines=n_lines,
            polars_dataframe_kwargs=polars_dataframe_kwargs,
            polars_write_parquet_kwargs=polars_write_parquet_kwargs,
            s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
            logger=logger,
        )

    def get_partitions(self):
        return get_partitions(
            s3_client=self.s3_client,
            s3dir_root=self.s3_loc.s3dir_staging,
        )

    def execute_compaction(
        self,
        partition: Partition,
        polars_write_parquet_kwargs: T_OPTIONAL_KWARGS = None,
        s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
        logger=dummy_logger,
    ):
        execute_compaction(
            partition=partition,
            s3_client=self.s3_client,
            s3_loc=self.s3_loc,
            update_at_col=self.extract_update_time.alias,
            polars_write_parquet_kwargs=polars_write_parquet_kwargs,
            s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
            logger=logger,
        )
