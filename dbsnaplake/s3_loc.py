# -*- coding: utf-8 -*-

"""
This module provides a structured way to manage and interact with S3 locations
in the ETL pipeline. It defines the S3Location class, which encapsulates
the logic for constructing and accessing various S3 paths used throughout the
ETL process.

The S3Location class handles two main areas:

1. Staging area: Temporary storage for processed data and manifests.
2. Data lake: Final storage location for optimized data.

This module is crucial for maintaining a consistent and organized S3 structure
throughout the pipeline, facilitating efficient data processing and retrieval.
"""

import typing as T
import dataclasses

from s3pathlib import S3Path

from .partition import Partition, get_partitions, encode_hive_partition
from .constants import (
    MANIFESTS_FOLDER,
    DATALAKE_FOLDER,
    SNAPSHOT_FILE_GROUPS_FOLDER,
    STAGING_FILE_GROUPS_FOLDER,
    PARTITION_FILE_GROUPS_FOLDER,
    MANIFEST_SUMMARY_FOLDER,
    MANIFEST_DATA_FOLDER,
)

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class S3Location:
    """
    A central class as a namespace to access all important S3 paths in the ETL
    pipeline.

    Example:
        >>> s3_loc = S3Location(
        ...     s3uri_staging="s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/"
        ...     s3uri_datalake="s3://bucket/prefix/datalake/mydatabase/mytable_2021_01_01_08_30_00/"
        ... )
        >>> s3_loc.s3dir_staging
        ...
        >>> s3_loc.s3dir_datalake
        ...
        >>> s3_loc.s3dir_staging_manifest
        ...
        >>> s3_loc.s3dir_snapshot_file_group_manifest
        ...
        >>> s3_loc.s3dir_snapshot_file_group_manifest_summary
        ...
        >>> s3_loc.s3dir_snapshot_file_group_manifest_data
        ...
        >>> s3_loc.s3dir_staging_file_group_manifest
        ...
        >>> s3_loc.s3dir_staging_file_group_manifest_summary
        ...
        >>> s3_loc.s3dir_staging_file_group_manifest_data
        ...
        >>> s3_loc.s3dir_partition_file_group_manifest
        ...
        >>> s3_loc.s3dir_partition_file_group_manifest_summary
        ...
        >>> s3_loc.s3dir_partition_file_group_manifest_data
        ...
        >>> s3_loc.s3dir_staging_datalake
        ...
    """

    s3uri_staging: str = dataclasses.field()
    s3uri_datalake: str = dataclasses.field()

    def __post_init__(self):
        self.s3uri_staging = self.s3dir_staging.uri
        self.s3uri_datalake = self.s3dir_datalake.uri

    @property
    def s3dir_staging(self) -> S3Path:
        """
        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/
        """
        return S3Path(self.s3uri_staging).to_dir()

    @property
    def s3dir_datalake(self) -> S3Path:
        """
        Example::

            s3://bucket/prefix/datalake/mydatabase/mytable_2021_01_01_08_30_00/
        """
        return S3Path(self.s3uri_datalake).to_dir()

    @property
    def s3dir_staging_manifest(self) -> S3Path:
        """
        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/
        """
        return (self.s3dir_staging / MANIFESTS_FOLDER).to_dir()

    @property
    def s3dir_snapshot_file_group_manifest(self) -> S3Path:
        """
        Where you store
        :class:`snapshot file group manifest files <dbsnaplake.snapshot_to_staging.DBSnapshotFileGroupManifestFile>`.

        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/snapshot-file-groups/
        """
        return (self.s3dir_staging_manifest / SNAPSHOT_FILE_GROUPS_FOLDER).to_dir()

    @property
    def s3dir_snapshot_file_group_manifest_summary(self) -> S3Path:
        """
        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/snapshot-file-groups/manifest-summary/
        """
        return (
            self.s3dir_snapshot_file_group_manifest / MANIFEST_SUMMARY_FOLDER
        ).to_dir()

    @property
    def s3dir_snapshot_file_group_manifest_data(self) -> S3Path:
        """
        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/snapshot-file-groups/manifest-data/
        """
        return (self.s3dir_snapshot_file_group_manifest / MANIFEST_DATA_FOLDER).to_dir()

    @property
    def s3dir_staging_file_group_manifest(self) -> S3Path:
        """
        Where you store
        :class:`staging file group manifest files <dbsnaplake.snapshot_to_staging.StagingFileGroupManifestFile>`.

        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/staging-file-groups/
        """
        return (self.s3dir_staging_manifest / STAGING_FILE_GROUPS_FOLDER).to_dir()

    @property
    def s3dir_staging_file_group_manifest_summary(self) -> S3Path:
        """
        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/staging-file-groups/manifest-summary/
        """
        return (
            self.s3dir_staging_file_group_manifest / MANIFEST_SUMMARY_FOLDER
        ).to_dir()

    @property
    def s3dir_staging_file_group_manifest_data(self) -> S3Path:
        """
        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/staging-file-groups/manifest-data/
        """
        return (self.s3dir_staging_file_group_manifest / MANIFEST_DATA_FOLDER).to_dir()

    @property
    def s3dir_partition_file_group_manifest(self) -> S3Path:
        """
        Where you store
        :class:`staging file group manifest files <dbsnaplake.staging_to_datalake.PartitionFileGroupManifestFile>`.

        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/partition-file-groups/
        """
        return (self.s3dir_staging_manifest / PARTITION_FILE_GROUPS_FOLDER).to_dir()

    @property
    def s3dir_partition_file_group_manifest_summary(self) -> S3Path:
        """
        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/partition-file-groups/manifest-summary/
        """
        return (
            self.s3dir_partition_file_group_manifest / MANIFEST_SUMMARY_FOLDER
        ).to_dir()

    @property
    def s3dir_partition_file_group_manifest_data(self) -> S3Path:
        """
        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/partition-file-groups/manifest-data/
        """
        return (
            self.s3dir_partition_file_group_manifest / MANIFEST_DATA_FOLDER
        ).to_dir()

    @property
    def s3dir_staging_datalake(self) -> S3Path:
        """
        Where you store the staged data that similar to your final
        datalake folder structure.

        Example::

            s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/datalake/
        """
        return (self.s3dir_staging / DATALAKE_FOLDER).to_dir()

    def iter_staging_datalake_partition(
        self,
        s3_client: "S3Client",
    ) -> T.List[Partition]:  # pragma: no cover
        """
        List all partitions in the staging datalake folder. So that you can
        plan how to compact small files in each partition into bigger files
        and move them to final datalake folder.
        """
        return get_partitions(
            s3_client=s3_client,
            s3dir_root=self.s3dir_staging_datalake,
        )

    def get_s3dir_staging_datalake_partition(
        self,
        kvs: T.Dict[str, str],
    ) -> S3Path:  # pragma: no cover
        """
        Get the S3 path for a specific partition in the staging datalake folder.
        """
        return (self.s3dir_staging_datalake / encode_hive_partition(kvs)).to_dir()

    def get_s3dir_datalake_partition(
        self,
        kvs: T.Dict[str, str],
    ) -> S3Path:  # pragma: no cover
        """
        Get the S3 path for a specific partition in the datalake folder.
        """
        return (self.s3dir_datalake / encode_hive_partition(kvs)).to_dir()
