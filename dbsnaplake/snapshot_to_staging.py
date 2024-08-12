# -*- coding: utf-8 -*-

"""
This module defines the abstraction and implementation of the transformation process
from Database Snapshot Data Files to Staging Data Files. It includes classes and
functions for managing manifest files, processing data groups, and handling data
transformations using Polars DataFrames.

Key components:

- :class:`DBSnapshotManifestFile`: Represents the full list of data files from the Database snapshot.
- :class:`DBSnapshotFileGroupManifestFile`: Represents a group of snapshot files.
- :class:`StagingFileGroupManifestFile`: Represents a group of staging files.
- :class:`DerivedColumn`: Defines how to derive new columns from the DataFrame.
- Various utility functions for data processing and S3 interactions.

.. seealso::

    `s3manifesto <https://s3manifesto.readthedocs.io/en/latest/>`_
"""

import typing as T
import uuid
import dataclasses

import polars as pl

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pass
from s3manifesto.api import KeyEnum, ManifestFile

from .typehint import T_EXTRACTOR
from .typehint import T_OPTIONAL_KWARGS
from .s3_loc import S3Location
from .polars_utils import write_data_file
from .polars_utils import group_by_partition
from .logger import dummy_logger


if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class DBSnapshotManifestFile(ManifestFile):
    """
    Represents the full list of data files from the Database snapshot.

    This class extends
    `ManifestFile <https://s3manifesto.readthedocs.io/en/latest/s3manifesto/manifest.html#module-s3manifesto.manifest>`_
    to provide specific functionality for handling database snapshot manifest files.
    """

    def split_into_groups(
        self,
        s3_loc: S3Location,
        s3_client: "S3Client",
        target_size: int = 100 * 1000 * 1000,  ## 100 MB
    ) -> T.List["DBSnapshotFileGroupManifestFile"]:
        """
        Split the full list of data files into groups of approximately equal size.

        :param s3_loc: S3 location information.
        :param s3_client: Boto3 S3 client.
        :param target_size: Target size for each group in bytes. Default is 100 MB.

        :return: List of file group manifest files.
        """
        db_snapshot_file_group_manifest_file_list = list()
        _lst = db_snapshot_file_group_manifest_file_list  # for shortening the code
        for ith, (data_file_list, total_size) in enumerate(
            self.group_files_into_tasks_by_size(target_size=target_size),
            start=1,
        ):
            db_snapshot_file_group_manifest_file = DBSnapshotFileGroupManifestFile.new(
                uri=s3_loc.s3dir_snapshot_file_group_manifest_data.joinpath(
                    f"manifest-data-{ith}.parquet"
                ).uri,
                uri_summary=s3_loc.s3dir_snapshot_file_group_manifest_summary.joinpath(
                    f"manifest-summary-{ith}.json"
                ).uri,
                size=total_size,
                data_file_list=data_file_list,
                calculate=True,
            )
            db_snapshot_file_group_manifest_file.write(s3_client)
            _lst.append(db_snapshot_file_group_manifest_file)
        return db_snapshot_file_group_manifest_file_list


@dataclasses.dataclass
class DBSnapshotFileGroupManifestFile(ManifestFile):
    """
    Represents a group of snapshot files from the database snapshot.

    This class extends
    `ManifestFile <https://s3manifesto.readthedocs.io/en/latest/s3manifesto/manifest.html#module-s3manifesto.manifest>`_
    to provide specific functionality for handling groups of snapshot files.
    """

    @classmethod
    def read_all_groups(
        cls,
        s3_loc: S3Location,
        s3_client: "S3Client",
    ) -> T.List["DBSnapshotFileGroupManifestFile"]:
        """
        Read all snapshot file group manifest files from the specified S3 location.

        :param s3_loc: S3 location information.
        :param s3_client: Boto3 S3 client.

        :returns: List of all file group manifest files.
        """
        s3path_list = s3_loc.s3dir_snapshot_file_group_manifest_summary.iter_objects(
            bsm=s3_client
        ).all()
        db_snapshot_file_group_manifest_file_list = [
            DBSnapshotFileGroupManifestFile.read(
                uri_summary=s3path.uri,
                s3_client=s3_client,
            )
            for s3path in s3path_list
        ]
        return db_snapshot_file_group_manifest_file_list


def batch_read_snapshot_data_file(
    db_snapshot_file_group_manifest_file: DBSnapshotFileGroupManifestFile,
    **kwargs,
) -> pl.DataFrame:
    """ """
    raise NotImplementedError


T_BatchReadSnapshotDataFileCallable = T.Callable[
    [DBSnapshotFileGroupManifestFile, ...], pl.DataFrame
]


@dataclasses.dataclass
class DerivedColumn:
    """
    Declares how to derive a new column from the DataFrame.

    :param extractor: A Polars expression or column name to derive the new column.
        If it is a polars expression, then it will be used to derive the new column.
        If it is a string, then use the given column, note that this column has to exist.
    :param alias: The name for the derived column.
    """

    extractor: T_EXTRACTOR = dataclasses.field()
    alias: str = dataclasses.field()


def add_derived_columns(
    df: pl.DataFrame,
    derived_columns: T.List[DerivedColumn],
) -> pl.DataFrame:
    """
    Add new columns to the DataFrame based on the given ``derived_columns`` specifications.

    :return: DataFrame with new columns added.

    **Example**

    >>> import polars as pl
    >>> df = pl.DataFrame({"id": ["id-1", "id-2", "id-3"]})
    >>> derived_columns = [
    ...     DerivedColumn(
    ...         extractor="id",
    ...         alias="record_id_1",
    ...     ),
    ...     DerivedColumn(
    ...         extractor=pl.col("id").str.split().list.last(),
    ...         alias="record_id_2",
    ...     ),
    ... ]
    >>> add_derived_columns(df, derived_columns)
    ┌──────┬─────────────┬─────────────┐
    │ id   ┆ record_id_1 ┆ record_id_2 │
    │ ---  ┆ ---         ┆ ---         │
    │ str  ┆ str         ┆ str         │
    ╞══════╪═════════════╪═════════════╡
    │ id-1 ┆ id-1        ┆ 1           │
    │ id-2 ┆ id-2        ┆ 2           │
    │ id-3 ┆ id-3        ┆ 3           │
    └──────┴─────────────┴─────────────┘
    """
    df_schema = df.schema
    for derived_column in derived_columns:
        # print(f"--- generate {derived_column.alias!r} column ---")  # for debug only
        if isinstance(derived_column.extractor, str):
            if derived_column.extractor not in df_schema:
                raise ValueError(
                    f"You plan to extract {derived_column.alias!r} "
                    f"from {derived_column.extractor!r} column, "
                    f"however, the column does not exist in the schema."
                )
            elif derived_column.extractor != derived_column.alias:
                df = df.with_columns(
                    pl.col(derived_column.extractor).alias(derived_column.alias)
                )
            else:
                pass
        else:
            if derived_column.alias in df_schema:
                raise ValueError(
                    f"You plan to extract {derived_column.alias!r}, "
                    f"however, the column already exists in the schema."
                )
            else:
                df = df.with_columns(
                    derived_column.extractor.alias(derived_column.alias)
                )
    return df


@dataclasses.dataclass
class StagingFileGroupManifestFile(ManifestFile):
    """
    Represents a group of staging files.
    """


def process_db_snapshot_file_group_manifest_file(
    db_snapshot_file_group_manifest_file: DBSnapshotFileGroupManifestFile,
    s3_client: "S3Client",
    s3_loc: S3Location,
    batch_read_snapshot_data_file_func: T_BatchReadSnapshotDataFileCallable,
    extract_record_id: DerivedColumn,
    extract_create_time: DerivedColumn,
    extract_update_time: DerivedColumn,
    extract_partition_keys: T.List[DerivedColumn],
    polars_write_parquet_kwargs: T_OPTIONAL_KWARGS = None,
    s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
    logger=dummy_logger,
) -> StagingFileGroupManifestFile:
    """
    Transform Snapshot Data Files to Staging Data Files, based on partition keys.

    This function processes a group of snapshot files, derives necessary columns,
    partitions the data if needed, and writes the results to S3 as staging files.


    :param db_snapshot_file_group_manifest_file: Manifest file for the snapshot group.
    :param df: DataFrame containing the snapshot data.
    :param s3_client: Boto3 S3 client.
    :param extract_record_id: Specification for deriving record ID.
    :param extract_create_time: Specification for deriving creation time.
    :param extract_update_time: Specification for deriving update time.
    :param extract_partition_keys: Specifications for deriving partition keys.
    :param s3_loc: S3 location information.
    :param polars_write_parquet_kwargs: Custom keyword arguments for Polars' write_parquet method.
        Default is ``dict(compression="snappy")``.
    :param s3pathlib_write_bytes_kwargs: Custom keyword arguments for S3Path's write_bytes method.
    :param logger: Logger object for logging operations.
    """
    # Derive more columns for data lake
    logger.info(
        "Derive record_id, create_time, update_time, and partition keys columns ..."
    )
    df = batch_read_snapshot_data_file_func(
        db_snapshot_file_group_manifest_file=db_snapshot_file_group_manifest_file,
    )
    df = add_derived_columns(
        df,
        [
            extract_record_id,
            extract_create_time,
            extract_update_time,
            *extract_partition_keys,
        ],
    )
    logger.info(f"  Dataframe Shape {df.shape}")

    # prepare variables for following operations
    if polars_write_parquet_kwargs is None:
        polars_write_parquet_kwargs = dict(compression="snappy")
    compression = polars_write_parquet_kwargs["compression"]
    filename = (
        f"{db_snapshot_file_group_manifest_file.fingerprint}.{compression}.parquet"
    )
    staging_data_file_list = list()

    # if we have partition keys, then we group data by partition keys
    # and write them to different partition (1 file per partition)
    if len(extract_partition_keys):
        logger.info("Group data by partition keys ...")
        partition_keys = [
            derived_column.alias for derived_column in extract_partition_keys
        ]
        # ----------------------------------------------------------------------
        # Method 1, split df into sub_df based on partition keys and
        # write them to different partition (1 file per partition)
        # ----------------------------------------------------------------------
        total_size = 0
        total_n_record = 0
        results = group_by_partition(
            df=df,
            s3dir=s3_loc.s3dir_staging_datalake,
            filename=filename,
            partition_keys=partition_keys,
            sort_by=[extract_update_time.alias],
        )
        logger.info(f"Will write data to {len(results)} partitions ...")
        for ith, (sub_df, s3path) in enumerate(results, start=1):
            logger.info(f"Write to {ith}th partition: {s3path.parent.uri}")
            logger.info(f"  s3uri: {s3path.uri}")
            logger.info(f"  preview at: {s3path.console_url}")
            size, n_record, etag = write_data_file(
                df=sub_df,
                s3_client=s3_client,
                s3path=s3path,
                polars_write_parquet_kwargs=polars_write_parquet_kwargs,
                s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
            )
            total_size += size
            total_n_record += n_record
            staging_data_file = {
                KeyEnum.URI: s3path.uri,
                KeyEnum.SIZE: size,
                KeyEnum.N_RECORD: n_record,
                KeyEnum.ETAG: etag,
            }
            staging_data_file_list.append(staging_data_file)
        # ----------------------------------------------------------------------
        # Method 2, Use ``pyarrow.parquet.write_to_dataset`` methods
        # ----------------------------------------------------------------------
        # pq.write_to_dataset(
        #     df.to_arrow(),
        #     root_path=s3dir_staging.uri,
        #     partition_cols=partition_keys,
        # )
    # if we don't have partition keys, then we write this file to the s3dir_staging
    else:
        logger.info("We don't have partition keys, write to single file ...")
        s3path = s3_loc.s3dir_staging_datalake.joinpath(filename)
        logger.info(f"Write to: {s3path.uri}")
        logger.info(f"  preview at: {s3path.console_url}")
        total_size, total_n_record, etag = write_data_file(
            df=df,
            s3_client=s3_client,
            s3path=s3path,
            polars_write_parquet_kwargs=polars_write_parquet_kwargs,
            s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
        )
        staging_data_file = {
            KeyEnum.URI: s3path.uri,
            KeyEnum.SIZE: total_size,
            KeyEnum.N_RECORD: total_n_record,
            KeyEnum.ETAG: etag,
        }
        staging_data_file_list.append(staging_data_file)

    staging_file_group_manifest_file = StagingFileGroupManifestFile.new(
        uri="",
        uri_summary="",
        data_file_list=staging_data_file_list,
        size=total_size,
        n_record=total_n_record,
        calculate=True,
    )
    fingerprint = staging_file_group_manifest_file.fingerprint
    s3path_manifest_data = (
        s3_loc.s3dir_staging_file_group_manifest_data
        / f"manifest-data-{fingerprint}.parquet"
    )
    s3path_manifest_summary = (
        s3_loc.s3dir_staging_file_group_manifest_summary
        / f"manifest-summary-{fingerprint}.json"
    )
    staging_file_group_manifest_file.uri = s3path_manifest_data.uri
    staging_file_group_manifest_file.uri_summary = s3path_manifest_summary.uri
    logger.info("Write generated files information to manifest file ...")
    logger.info(f"  Write to manifest summary: {s3path_manifest_summary.uri}")
    logger.info(f"    preview at: {s3path_manifest_summary.console_url}")
    logger.info(f"  Write to manifest data: {s3path_manifest_data.uri}")
    logger.info(f"    preview at: {s3path_manifest_data.console_url}")
    staging_file_group_manifest_file.write(s3_client=s3_client)
    return staging_file_group_manifest_file
