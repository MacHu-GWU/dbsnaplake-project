# -*- coding: utf-8 -*-

"""
This module defines the abstraction of the transformation process from
Staging Data File to Final Datalake.
"""

import typing as T
import math
import dataclasses

import polars as pl
from s3pathlib import S3Path

from .typehint import T_OPTIONAL_KWARGS
from .constants import S3_METADATA_KEY_N_RECORDS, S3_METADATA_KEY_STAGING_PARTITION
from .logger import dummy_logger
from .partition import Partition
from .polars_utils import write_parquet_to_s3
from .utils import repr_data_size

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class DatalakeDataFile:
    """
    Represent a parquet data file in datalake. It is the result of compaction
    of multiple :class:`StagingDataFile`.

    :param uri: S3 URI of the data file.
    :param n_records: Number of records in the data file.
    :param staging_partition_uri: which staging partition the data file comes from.
    """

    uri: str = dataclasses.field()
    n_records: int = dataclasses.field()
    staging_partition_uri: str = dataclasses.field()

    @property
    def s3path(self) -> S3Path:
        return S3Path.from_s3_uri(self.uri)

    def write_parquet_to_s3(
        self,
        df: pl.DataFrame,
        s3_client: "S3Client",
        polars_write_parquet_kwargs: T_OPTIONAL_KWARGS = None,
        s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
    ):
        if s3pathlib_write_bytes_kwargs is None:
            s3pathlib_write_bytes_kwargs = {}
        s3pathlib_write_bytes_kwargs["content_type"] = "application/x-parquet"
        more_metadata = {
            S3_METADATA_KEY_N_RECORDS: str(df.shape[0]),
            S3_METADATA_KEY_STAGING_PARTITION: self.staging_partition_uri,
        }
        if "metadata" in s3pathlib_write_bytes_kwargs:
            s3pathlib_write_bytes_kwargs["metadata"].update(more_metadata)
        else:
            s3pathlib_write_bytes_kwargs["metadata"] = more_metadata
        return write_parquet_to_s3(
            df=df,
            s3path=self.s3path,
            s3_client=s3_client,
            polars_write_parquet_kwargs=polars_write_parquet_kwargs,
            s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
        )


def execute_compaction(
    partition: "Partition",
    s3_client: "S3Client",
    s3dir_staging: S3Path,
    s3dir_datalake: S3Path,
    update_at_col: str,
    polars_write_parquet_kwargs: T_OPTIONAL_KWARGS = None,
    s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
    logger=dummy_logger,
) -> T.List[DatalakeDataFile]:
    # scan partition, get all staging data file
    logger.info(f"Execute compaction on partition: {partition.data}")
    logger.info(f"  s3uri: {partition.uri}")
    logger.info(f"  preview at: {partition.s3path.console_url}")

    logger.info("List all staging data files ...")
    s3dir = S3Path(partition.uri)
    s3path_list = (
        s3dir.iter_objects().filter(lambda x: x.basename.endswith(".parquet")).all()
    )
    total_size = sum([s3path.size for s3path in s3path_list])
    total_records = sum(
        [int(s3path.metadata[S3_METADATA_KEY_N_RECORDS]) for s3path in s3path_list]
    )
    logger.info(f"  Total files: {len(s3path_list)}")
    logger.info(f"  Total size: {repr_data_size(total_size)}")
    logger.info(f"  Total records: {total_records}")

    # Read all the parquet files in the partition, sort by update_at_col
    logger.info(f"Read all staging data files and sort by {update_at_col!r} column ...")
    df: pl.DataFrame = (
        pl.scan_parquet(
            f"{partition.uri}*.parquet",
        )
        .sort(by=update_at_col)
        .collect()
    )

    # prepare variables for following operations
    if polars_write_parquet_kwargs is None:
        polars_write_parquet_kwargs = dict(compression="snappy")
    compression = polars_write_parquet_kwargs["compression"]
    _relpath = s3dir.relative_to(s3dir_staging)
    s3dir_datalake_partition = s3dir_datalake.joinpath(_relpath)
    logger.info("Start writing merged files to datalake partition ...")
    logger.info(f"  s3uri: {s3dir_datalake_partition.uri}")
    logger.info(f"  preview at: {s3dir_datalake_partition.console_url}")

    # analyze the size of the files and calculate how many DatalakeDataFile to produce
    expect_size = 128000000
    n_files = math.ceil(total_size / expect_size)
    n_records_per_datalake_data_file = int(total_records / n_files)
    size_per_datalake_data_file = int(total_size / n_files)

    # Merge small parquet files into larger files
    logger.info(f"  Will produce {n_files} files.")
    logger.info(f"  each file will have {n_records_per_datalake_data_file} records.")
    logger.info(
        f"  each file will be around {repr_data_size(size_per_datalake_data_file)}."
    )
    datalake_data_file_list = list()
    if n_files == 1:
        s3path = s3dir_datalake_partition.joinpath(f"1.{compression}.parquet")
        logger.info(f"  Write to {s3path.uri} ...")
        datalake_data_file = DatalakeDataFile(
            uri=s3path.uri,
            n_records=df.shape[0],
            staging_partition_uri=partition.uri,
        )
        datalake_data_file.write_parquet_to_s3(
            df=df,
            s3_client=s3_client,
            polars_write_parquet_kwargs=polars_write_parquet_kwargs,
            s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
        )
        datalake_data_file_list.append(datalake_data_file)
    else:
        for ith, sub_df in enumerate(
            df.iter_slices(n_rows=n_records_per_datalake_data_file),
            start=1,
        ):
            s3path = s3dir_datalake_partition.joinpath(f"{ith}.{compression}.parquet")
            logger.info(f"  Write to {s3path.uri} ...")
            datalake_data_file = DatalakeDataFile(
                uri=s3path.uri,
                n_records=sub_df.shape[0],
                staging_partition_uri=partition.uri,
            )
            datalake_data_file.write_parquet_to_s3(
                df=sub_df,
                s3_client=s3_client,
                polars_write_parquet_kwargs=polars_write_parquet_kwargs,
                s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
            )
            datalake_data_file_list.append(datalake_data_file)
    logger.info("Done.")
    return datalake_data_file_list
