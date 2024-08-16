# -*- coding: utf-8 -*-

"""
Data Lake Validation Module

This module provides utilities for validating and analyzing the contents of a data lake.
It includes classes for representing partitions and validation results, as well as
a function to perform the actual validation process.
"""

import typing as T
import json
import dataclasses

import polars as pl
from s3pathlib import S3Path
from .utils import repr_data_size
from .s3_loc import S3Location
from .partition import Partition
from .partition import extract_partition_data
from .snapshot_to_staging import DBSnapshotManifestFile
from .logger import dummy_logger

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class Partition:
    data: T.Dict[str, str]
    n_files: int
    total_size: int
    total_size_4_human: str
    total_n_record: T.Optional[int]


@dataclasses.dataclass
class ValidateDatalakeResult:
    """
    Encapsulates the results of a data lake validation process.

    This class stores comparison data between the original snapshot and the
    processed data lake, including file counts, sizes, and record counts.

    :param before_n_files: Number of files in the original snapshot.
    :param before_total_size: Total size of the original snapshot in bytes.
    :param before_total_size_4_human: Human-readable representation of the original snapshot size.
    :param before_total_n_record: Total number of records in the original snapshot.
    :param after_n_files: Number of files in the processed data lake.
    :param after_total_size: Total size of the processed data lake in bytes.
    :param after_total_size_4_human: Human-readable representation of the processed data lake size.
    :param after_total_n_record: Total number of records in the processed data lake.
    :param n_partition: Number of partitions in the processed data lake.
    :param partitions): List of Partition objects representing each partition in the data lake.
    """

    before_n_files: int
    before_total_size: int
    before_total_size_4_human: str
    before_total_n_record: int
    after_n_files: int
    after_total_size: int
    after_total_size_4_human: str
    after_total_n_record: T.Optional[int]
    n_partition: int
    partitions: T.List[Partition]


def validate_datalake(
    s3_client: "S3Client",
    s3_loc: S3Location,
    db_snapshot_manifest_file: DBSnapshotManifestFile,
    column: T.Optional[str] = None,
    logger=dummy_logger,
) -> ValidateDatalakeResult:
    """
    Validates the data lake by scanning its contents and collecting statistics.

    This function compares the original database snapshot with the processed data lake,
    providing detailed information about file counts, sizes, and record counts.

    :param s3_client: An initialized boto3 S3 client for S3 operations.
    :param s3_loc: S3 location information for the data lake.
    :param db_snapshot_manifest_file: Manifest file of the original database snapshot.
    :param column: Name of the column used to count the number of records. This
        column has to exist in all rows. If not provided, then it will not include
        the record count in the validation result.

    .. note::

        We don't use previous manifest data to validate the datalake. We only use
        the current snapshot data to validate the datalake.

    .. note::

        The count n record feature is not available in unit test, because
        the polars.scan_parquet method is not working well with moto (mock).
    """
    s3dir_root = s3_loc.s3dir_datalake
    logger.info(f"Validate datalake at: {s3dir_root.uri}")
    logger.info(f"Scan all files ...")
    s3path_list = s3dir_root.iter_objects(bsm=s3_client).all()
    s3dir_uri_list = list()
    partition_to_file_list_mapping: T.Dict[str, T.List[S3Path]] = dict()
    len_s3dir_root = len(s3dir_root.uri)
    after_n_files = 0
    after_total_size = 0
    for s3path in s3path_list:
        s3dir_uri = s3path.parent.uri
        # make sure either it is the s3dir_root or it has "=" character in it
        if ("=" in s3dir_uri.split("/")[-2]) or (len(s3dir_uri) == len_s3dir_root):
            s3dir_uri_list.append(s3dir_uri)
            after_n_files += 1
            after_total_size += s3path.size
            try:
                partition_to_file_list_mapping[s3dir_uri].append(s3path)
            except KeyError:
                partition_to_file_list_mapping[s3dir_uri] = [s3path]

    logger.info(f"  Got {len(partition_to_file_list_mapping)} partitions.")
    logger.info(f"Collect per partition information ...")
    after_total_n_record = 0
    partitions = list()
    for s3dir_uri, s3path_list in partition_to_file_list_mapping.items():
        s3dir = S3Path.from_s3_uri(s3dir_uri)
        partition_data = extract_partition_data(s3dir_root, s3dir)
        if column is not None:
            n_record = (
                pl.scan_parquet(
                    [s3path.uri for s3path in s3path_list],
                )
                .select(pl.col(column))
                .count()
                .collect()[column][0]
            )
            after_total_n_record += n_record
        else:
            n_record = None
        total_size = sum(s3path.size for s3path in s3path_list)
        partition = Partition(
            data=partition_data,
            n_files=len(s3path_list),
            total_size=total_size,
            total_size_4_human=repr_data_size(total_size),
            total_n_record=n_record,
        )
        logger.info(f"Statistics infor for partition {partition_data}:")
        logger.info(f"  n_files = {partition.n_files}")
        logger.info(f"  total_size = {partition.total_size}")
        logger.info(f"  total_size_4_human = {partition.total_size_4_human}")
        logger.info(f"  total_n_record = {partition.total_n_record}")
        partitions.append(partition)

    if column is None:
        after_total_n_record = None

    result = ValidateDatalakeResult(
        before_n_files=len(db_snapshot_manifest_file.data_file_list),
        before_total_size=db_snapshot_manifest_file.size,
        before_total_size_4_human=repr_data_size(db_snapshot_manifest_file.size),
        before_total_n_record=db_snapshot_manifest_file.n_record,
        after_n_files=after_n_files,
        after_total_size=after_total_size,
        after_total_size_4_human=repr_data_size(after_total_size),
        after_total_n_record=after_total_n_record,
        n_partition=len(partitions),
        partitions=partitions,
    )
    logger.info(f"Statistics infor for datalake {s3dir_root.uri}:")
    logger.info(f"{result.before_n_files = }")
    logger.info(f"{result.before_total_size = }")
    logger.info(f"{result.before_total_size_4_human = }")
    logger.info(f"{result.before_total_n_record = }")
    logger.info(f"{result.after_n_files = }")
    logger.info(f"{result.after_total_size = }")
    logger.info(f"{result.after_total_size_4_human = }")
    logger.info(f"{result.after_total_n_record = }")
    logger.info(f"{result.n_partition = }")

    s3path = s3_loc.s3path_validate_datalake_result
    logger.info(f"Write datalake statistics to {s3path.uri = }")
    logger.info(f"  Preview at: {s3path.console_url}")
    result_data = dataclasses.asdict(result)
    content = json.dumps(result_data, indent=4)
    s3path.write_text(
        content,
        content_type="application/json",
        bsm=s3_client,
    )
    return result
