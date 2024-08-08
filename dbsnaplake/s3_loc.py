# -*- coding: utf-8 -*-

"""
S3 Location
"""

import typing as T
import dataclasses

from s3pathlib import S3Path

from .partition import Partition, get_partitions, encode_hive_partition

if T.TYPE_CHECKING: # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class S3Location:
    """
    在从 DB Snapshot 到 DataLake 的过程中, 我们的策略是是将数据按照 partition 放到
    staging 目录, 然后再将 staging 目录中的数据进行 compaction, 再写入到 datalake 目录.
    这个类用于管理 staging 和 datalake 目录的 S3 URI.

    .. code-block:: python

        s3://bucket/prefix/staging/
            ${database_name}/
                ${schema_name}/
                    ${table_name}/
                        {snapshot_id}/
                            ${partition_key1}=${partition_key1_value}/
                                ${partition_key2}=${partition_key2_value}/
                                    .../
                                        ${staging_data_file}

        s3://bucket/prefix/datalake/
            ${database_name}/
                ${schema_name}/
                    ${table_name}/
                        ${partition_key1}=${partition_key1_value}/
                                ${partition_key2}=${partition_key2_value}/
                                    .../
                                        ${data_file}

    """

    s3uri_staging: str = dataclasses.field()
    s3uri_datalake: str = dataclasses.field()

    def __post_init__(self):
        self.s3uri_staging = self.s3dir_staging.uri
        self.s3uri_datalake = self.s3dir_datalake.uri

    @property
    def s3dir_staging(self) -> S3Path:
        return S3Path(self.s3uri_staging).to_dir()

    @property
    def s3dir_datalake(self) -> S3Path:
        return S3Path(self.s3uri_datalake).to_dir()

    def get_s3dir_staging_partition(self, kvs: T.Dict[str, str]) -> S3Path:
        return (self.s3dir_staging / encode_hive_partition(kvs)).to_dir()

    def get_s3dir_datalake_partition(self, kvs: T.Dict[str, str]) -> S3Path:
        return (self.s3dir_staging / encode_hive_partition(kvs)).to_dir()

    def iter_staging_partition(self, s3_client: "S3Client") -> T.List[Partition]:
        return get_partitions(s3_client=s3_client, s3dir_root=self.s3dir_staging)
