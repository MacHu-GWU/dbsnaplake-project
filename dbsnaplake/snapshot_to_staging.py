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

from __future__ import annotations
import typing as T
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
    Represents the full list of data files from the Database snapshot. We will
    have only one DB snapshot manifest file in the data pipeline. And this is
    where the data pipeline starts.

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

        .. seealso::

            :class:`DBSnapshotFileGroupManifestFile`
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
    This class is a subgroup of :class:`DBSnapshotManifestFile`, created by
    breaking down a larger snapshot into more manageable units.

    This class extends
    `ManifestFile <https://s3manifesto.readthedocs.io/en/latest/s3manifesto/manifest.html#module-s3manifesto.manifest>`_
    to provide specific functionality for handling groups of snapshot files.

    .. seealso::

        :meth:`DBSnapshotManifestFile.split_into_groups`
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
    """
    Loads multiple snapshot data files into an in-memory
    `Polars DataFrame <https://docs.pola.rs/api/python/stable/reference/dataframe/index.html>`_
    and applies custom transformations.

    This user-defined function serves as a central point for data processing and
    transformation in the pipeline. It allows for various operations such as
    data validation, column manipulation (add/rename/drop), joining with external
    data sources, data type casting, filtering, and aggregation.

    :param db_snapshot_file_group_manifest_file: :class:`DBSnapshotFileGroupManifestFile`
        Object containing references to the snapshot data files to be processed.

    :return: A Polars DataFrame containing the processed snapshot data.

    Below is an example to read data from multiple NDJson files and align them:

    >>> import polars as pl
    >>> def batch_read_snapshot_data_file(
    ...     db_snapshot_file_group_manifest_file: DBSnapshotFileGroupManifestFile,
    ...     **kwargs,
    ... ) -> pl.DataFrame:
    ...     sub_df_list = list()
    ...     for data_file in db_snapshot_file_group_manifest_file.data_file_list:
    ...         sub_df = pl.read_ndjson(data_file["uri"])
    ...         # arbitrary transofmration logic here
    ...         sub_df = sub_df.with_columns(pl.col("OrderId").alias("record_id"))
    ...         sub_df_list.append(sub_df)
    ...     df = pl.concat(sub_df_list)
    ...     return df

    .. seealso::

        - `Polars Input/Output Documentation <https://docs.pola.rs/user-guide/io/>`_:
          For reading data from different file formats and cloud storage.
        - :func:`process_db_snapshot_file_group_manifest_file`: Related processing function.
    """
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
    Represents a group of staging files derived from a single
    :class:`DBSnapshotFileGroupManifestFile`.

    This class manages the output of the partitioning process applied to a
    :class:`DBSnapshotFileGroupManifestFile`. It stores references to multiple
    data files, each corresponding to a specific partition.

    Key characteristics:

    1. One-to-one relationship: Each :class:`DBSnapshotFileGroupManifestFile`
        generates exactly one :class:`StagingFileGroupManifestFile`.
    2. Partitioning: The original snapshot data is divided into multiple partitions.
    3. File generation: One data file is created for each partition.
    4. Storage: This manifest file stores the list of all generated data files.

    The :class:`StagingFileGroupManifestFile` serves as an index or catalog for
    the partitioned and processed data, facilitating efficient data retrieval and
    management in subsequent data lake operations.

    .. seealso::

        - :class:`DBSnapshotFileGroupManifestFile`
        - :func:`process_db_snapshot_file_group_manifest_file`
    """


def process_db_snapshot_file_group_manifest_file(
    db_snapshot_file_group_manifest_file: DBSnapshotFileGroupManifestFile,
    s3_client: "S3Client",
    s3_loc: S3Location,
    batch_read_snapshot_data_file_func: T_BatchReadSnapshotDataFileCallable,
    extract_record_id: T.Optional[DerivedColumn] = None,
    extract_create_time: T.Optional[DerivedColumn] = None,
    extract_update_time: T.Optional[DerivedColumn] = None,
    extract_partition_keys: T.Optional[T.List[DerivedColumn]] = None,
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
    :param s3_loc: S3 location information.
    :param batch_read_snapshot_data_file_func:
    :param extract_record_id: (optional) Specification for deriving record ID.
    :param extract_create_time: (optional) Specification for deriving creation time.
    :param extract_update_time: (optional) Specification for deriving update time.
    :param extract_partition_keys: (optional) Specifications for deriving partition keys.
    :param polars_write_parquet_kwargs: Custom keyword arguments for Polars' write_parquet method.
        Default is ``dict(compression="snappy")``.
    :param s3pathlib_write_bytes_kwargs: Custom keyword arguments for S3Path's write_bytes method.
    :param logger: Logger object for logging operations.

    :return: single :class:`StagingFileGroupManifestFile` object
    """
    # Derive more columns for data lake
    logger.info(
        "Derive record_id, create_time, update_time, and partition keys columns ..."
    )
    df = batch_read_snapshot_data_file_func(
        db_snapshot_file_group_manifest_file=db_snapshot_file_group_manifest_file,
    )
    if extract_partition_keys is None:
        extract_partition_keys = list()
    derived_columns = [
        extract_record_id,
        extract_create_time,
        extract_update_time,
        *extract_partition_keys,
    ]
    derived_columns = [col for col in derived_columns if col is not None]
    if len(derived_columns):
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
