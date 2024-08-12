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
from s3manifesto.api import KeyEnum, ManifestFile

from .typehint import T_RECORD
from .typehint import T_DF_SCHEMA
from .typehint import T_EXTRACTOR
from .typehint import T_OPTIONAL_KWARGS
from .utils import repr_data_size
from .s3_loc import S3Location
from .partition import Partition
from .partition import extract_partition_data
from .partition import encode_hive_partition
from .partition import get_s3dir_partition
from .partition import get_partitions
from .polars_utils import write_parquet_to_s3
from .polars_utils import write_data_file
from .polars_utils import read_parquet_from_s3
from .polars_utils import read_many_parquet_from_s3
from .polars_utils import group_by_partition
from .compaction import get_merged_schema
from .compaction import harmonize_schemas
from .logger import dummy_logger
from .snapshot_to_staging import DBSnapshotManifestFile
from .snapshot_to_staging import DBSnapshotFileGroupManifestFile
from .snapshot_to_staging import batch_read_snapshot_data_file
from .snapshot_to_staging import DerivedColumn
from .snapshot_to_staging import StagingFileGroupManifestFile
from .snapshot_to_staging import process_db_snapshot_file_group_manifest_file

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def extract_s3dir(
    s3uri_col_name: str,
    s3dir_col_name: str,
) -> pl.Expr:
    """
    Let's say there is a DataFrame with a column named "s3uri_col_name" that
    contains a lot of s3 uri strings like this "s3://bucket/path/to/file".
    This function returns a polars expression that can generate a new column
    named "s3dir_col_name" that contains the directory part of the s3 uri,
    like this "s3://bucket/path/to/" (with the trailing slash).
    """
    return pl.concat_str(
        [
            pl.col(s3uri_col_name)
            .str.split("/")
            .list.slice(
                0,
                pl.col(s3uri_col_name).str.split("/").list.len() - 1,
            )
            .list.join("/"),
            pl.lit("/"),
        ]
    ).alias(s3dir_col_name)


PARTITION_URI = "partition_uri"


@dataclasses.dataclass
class PartitionFileGroupManifestFile(ManifestFile):
    @classmethod
    def plan_partition_compaction(
        cls,
        s3_loc: S3Location,
        s3_client: "S3Client",
        target_size: int = 128_000_000,  # 128 MB
    ):
        s3path_list = s3_loc.s3dir_staging_file_group_manifest_data.iter_objects(
            bsm=s3_client
        ).all()
        df = read_many_parquet_from_s3(
            s3path_list=s3path_list,
            s3_client=s3_client,
        )
        df = df.with_columns(
            extract_s3dir(s3uri_col_name=KeyEnum.URI, s3dir_col_name=PARTITION_URI),
        )
        partition_file_group_manifest_file_list = list()
        for ith_partition, ((partition_uri,), sub_df) in enumerate(
            df.group_by(*[PARTITION_URI]),
            start=1,
        ):
            sub_df = sub_df.drop([PARTITION_URI])
            data_file_list = sub_df.to_dicts()
            master_partition_file_group_manifest_file = (
                PartitionFileGroupManifestFile.new(
                    uri="",
                    uri_summary="",
                    data_file_list=data_file_list,
                    calculate=True,
                )
            )
            for ith, (
                file_group,
                total_size,
            ) in enumerate(
                master_partition_file_group_manifest_file.group_files_into_tasks_by_size(
                    target_size=target_size,
                ),
                start=1,
            ):
                partition_file_group_manifest_file = PartitionFileGroupManifestFile.new(
                    uri=s3_loc.s3dir_partition_file_group_manifest_data.joinpath(
                        f"manifest-data-{ith_partition}-{ith}.parquet"
                    ).uri,
                    uri_summary=s3_loc.s3dir_partition_file_group_manifest_summary.joinpath(
                        f"manifest-summary-{ith_partition}-{ith}.parquet"
                    ).uri,
                    data_file_list=file_group,
                    details={
                        PARTITION_URI: partition_uri,
                    },
                    calculate=True,
                )
                partition_file_group_manifest_file.write(s3_client=s3_client)
                partition_file_group_manifest_file_list.append(
                    partition_file_group_manifest_file
                )
        return partition_file_group_manifest_file_list

    @classmethod
    def read_many(
        cls,
        s3_loc: S3Location,
        s3_client: "S3Client",
    ):
        s3path_list = s3_loc.s3dir_partition_file_group_manifest_summary.iter_objects(
            bsm=s3_client,
        ).all()
        partition_file_group_manifest_file_list = [
            PartitionFileGroupManifestFile.read(
                uri_summary=s3path.uri,
                s3_client=s3_client,
            )
            for s3path in s3path_list
        ]
        return partition_file_group_manifest_file_list

    # for s3path_manifest_summary in s3path_manifest_summary_list:
    #     manifest_file = ManifestFile.read(
    #         uri_summary=s3path_manifest_summary.uri,
    #         s3_client=s3_client,
    #     )
    #     for data_file in manifest_file.data_file_list:
    #         s3path = data_file[KeyEnum.URI]
    #         try:
    #             partition_mapping[s3path.parent.uri].append(data_file)
    #         except KeyError:
    #             partition_mapping[s3path.parent.uri] = [data_file]
    #
    # for s3uri_partition, data_file_list in partition_mapping.items():
    #     manifest_file = ManifestFile.new(
    #         uri_summary=s3uri_partition,
    #         data_file_list=data_file_list,
    #     )
    #
    # data_file_list = list()
    # for s3path in partition.list_parquet_files():
    #     staging_data_file = StagingDataFile(
    #         uri=s3path.uri,
    #         size=s3path.size,
    #     )
    #     data_file_list.append(staging_data_file)
    # manifest_file = ManifestFile(
    #     uri="...",
    #     data_file_list=data_file_list,
    # )
    # data_file_group_list = manifest_file.group_files_into_tasks(
    #     target_size=128_000_000,  # 128MB is optimal for parquet file
    # )
    # return data_file_group_list


# @dataclasses.dataclass
# class DatalakeDataFile:
#     """
#     Represent a parquet data file in datalake. It is the result of compaction
#     of multiple :class:`StagingDataFile`.
#
#     :param uri: S3 URI of the data file.
#     :param n_records: Number of records in the data file.
#     :param staging_partition_uri: which staging partition the data file comes from.
#     """
#
#     uri: str = dataclasses.field()
#     n_records: int = dataclasses.field()
#     staging_partition_uri: str = dataclasses.field()
#
#     @property
#     def s3path(self) -> S3Path:
#         return S3Path.from_s3_uri(self.uri)
#
#     def write_parquet_to_s3(
#         self,
#         df: pl.DataFrame,
#         s3_client: "S3Client",
#         polars_write_parquet_kwargs: T_OPTIONAL_KWARGS = None,
#         s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
#     ):
#         if s3pathlib_write_bytes_kwargs is None:
#             s3pathlib_write_bytes_kwargs = {}
#         s3pathlib_write_bytes_kwargs["content_type"] = "application/x-parquet"
#         more_metadata = {
#             S3_METADATA_KEY_N_RECORDS: str(df.shape[0]),
#             S3_METADATA_KEY_STAGING_PARTITION: self.staging_partition_uri,
#         }
#         if "metadata" in s3pathlib_write_bytes_kwargs:
#             s3pathlib_write_bytes_kwargs["metadata"].update(more_metadata)
#         else:
#             s3pathlib_write_bytes_kwargs["metadata"] = more_metadata
#         return write_parquet_to_s3(
#             df=df,
#             s3path=self.s3path,
#             s3_client=s3_client,
#             polars_write_parquet_kwargs=polars_write_parquet_kwargs,
#             s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
#         )


def execute_compaction(
    partition_file_group_manifest_file: PartitionFileGroupManifestFile,
    s3_client: "S3Client",
    s3_loc: S3Location,
    update_at_col: str,
    polars_write_parquet_kwargs: T_OPTIONAL_KWARGS = None,
    s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
    logger=dummy_logger,
) -> S3Path:
    partition_uri = partition_file_group_manifest_file.details[PARTITION_URI]
    s3dir_partition = S3Path.from_s3_uri(partition_uri)
    logger.info(f"Execute compaction on partition: {partition_uri}")
    logger.info(f"  preview output at: {s3dir_partition.console_url}")

    # Read all the data file in this manifest, sort by update_at_col
    logger.info(f"Read all staging data files and sort by {update_at_col!r} column ...")
    sub_df_list = list()
    for data_file in partition_file_group_manifest_file.data_file_list:
        uri = data_file[KeyEnum.URI]
        s3path = S3Path.from_s3_uri(uri)
        logger.info(f"  Read: {uri}")
        sub_df = read_parquet_from_s3(s3path=s3path, s3_client=s3_client)
        sub_df_list.append(sub_df)
    df = pl.concat(sub_df_list)

    _relpath = s3dir_partition.relative_to(s3_loc.s3dir_staging_datalake)
    s3dir_datalake_partition = s3_loc.s3dir_datalake.joinpath(_relpath)
    if polars_write_parquet_kwargs is None:
        polars_write_parquet_kwargs = dict(compression="snappy")
    compression = polars_write_parquet_kwargs["compression"]
    filename = f"{partition_file_group_manifest_file.fingerprint}.{compression}.parquet"
    s3path = s3dir_datalake_partition.joinpath(filename)
    logger.info(f"Writ merged files to {s3path.uri} ...")
    logger.info(f"  preview at: {s3path.console_url}")
    write_data_file(
        df=df,
        s3path=s3path,
        s3_client=s3_client,
        polars_write_parquet_kwargs=polars_write_parquet_kwargs,
        s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
    )
    return s3path
