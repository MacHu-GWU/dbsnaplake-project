# -*- coding: utf-8 -*-

"""

"""

import typing as T
import dataclasses

import polars as pl
from s3pathlib import S3Path
from .s3_loc import S3Location
from .partition import Partition
from .partition import extract_partition_data
from .snapshot_to_staging import DBSnapshotManifestFile

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class Partition:
    data: T.Dict[str, str]
    n_files: int
    total_size: int
    total_n_record: int


@dataclasses.dataclass
class ValidateDatalakeResult:
    before_n_files: int
    before_total_size: int
    before_total_n_record: int
    after_n_files: int
    after_total_size: int
    after_total_n_record: int
    partitions: T.List[Partition]


def validate_datalake(
    s3_client: "S3Client",
    s3_loc: S3Location,
    id_col: str,
    db_snapshot_manifest_file: DBSnapshotManifestFile,
) -> ValidateDatalakeResult:
    """
    Scan the final data lake, and collect the statistics information of the data lake.

    .. note::

        We don't use previous manifest data to validate the datalake. We only use
        the current snapshot data to validate the datalake.
    """
    s3dir_root = s3_loc.s3dir_datalake
    s3path_list = s3dir_root.iter_objects(bsm=s3_client).all()
    # s3_uri_set = {s3path.parent.uri for s3path in s3path_list}
    s3dir_uri_list = list()
    partition_to_file_list_mapping: T.Dict[str, T.List[S3Path]] = dict()
    len_s3dir_root = len(s3dir_root.uri)
    after_n_files = 0
    after_total_size = 0
    for s3path in s3path_list:
        s3dir_uri = s3path.parent.uri
        if ("=" in s3dir_uri.split("/")[-2]) or (len(s3dir_uri) == len_s3dir_root):
            s3dir_uri_list.append(s3dir_uri)
            after_n_files += 1
            after_total_size += s3path.size
            try:
                partition_to_file_list_mapping[s3dir_uri].append(s3path)
            except KeyError:
                partition_to_file_list_mapping[s3dir_uri] = [s3path]

    # make sure either it is the s3dir_root or it has "=" character in it
    after_total_n_record = 0
    partitions = list()
    for s3dir_uri, s3path_list in partition_to_file_list_mapping:
        s3dir = S3Path.from_s3_uri(s3dir_uri)
        partition_data = extract_partition_data(s3dir_root, s3dir)
        v = (
            pl.scan_parquet(
                [s3path.uri for s3path in s3path_list],
            )
            .select(pl.col(id_col))
            .count()
            .collect()
        )
        print(v)
        partition = Partition(
            data=partition_data,
            n_files=len(s3path_list),
            total_size=sum(s3path.size for s3path in s3path_list),
            total_n_record=0,
        )
        partitions.append(partition)

    validate_datalake_result = ValidateDatalakeResult(
        before_n_files=len(db_snapshot_manifest_file.data_file_list),
        before_total_size=db_snapshot_manifest_file.size,
        before_total_n_record=db_snapshot_manifest_file.n_record,
        after_n_files=after_n_files,
        after_total_size=after_total_size,
        after_total_n_record=after_total_n_record,
        partitions=partitions,
    )

    return validate_datalake_result
