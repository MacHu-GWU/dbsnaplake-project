# -*- coding: utf-8 -*-

"""
This module defines the abstraction of the transformation process from
Snapshot Data File to Staging Data File.
"""

import typing as T
import uuid
import dataclasses

import polars as pl

try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pass
from s3pathlib import S3Path
from s3manifesto.api import T_DATA_FILE, KeyEnum, ManifestFile

from .typehint import T_EXTRACTOR, T_OPTIONAL_KWARGS
from .constants import (
    DBSNAPLAKE_MANIFEST_FOLDER,
)
from .logger import dummy_logger
from .polars_utils import write_data_file, group_by_partition

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def batch_read_snapshot_data_file(
    manifest_file: ManifestFile,
    *args,
    **kwargs,
):
    """ """
    raise NotImplementedError


@dataclasses.dataclass
class DerivedColumn:
    """
    Declare how you want to derive a new column from the DataFrame.

    :param extractor: if it is a polars expression, then it will be used
        to derive the new column. if it is a string, then use the given column,
        note that this column has to exist.
    :param alias: if you want to rename the derived column, then specify the alias.
    """

    extractor: T_EXTRACTOR = dataclasses.field()
    alias: str = dataclasses.field()


def generate_more_columns(
    df: pl.DataFrame,
    derived_columns: T.List[DerivedColumn],
) -> pl.DataFrame:
    """
    Generate more columns based on the given derived_columns.

    For example:

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
        >>> generate_more_columns(df, derived_columns)
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


def get_filename(filename: T.Optional[str]) -> str:
    """
    A valid file name should not contain any dot and file extension,
    the write engine will append the file extension based on the compression
    automatically. This function will remove anything after the first dot.

    Example:

        >>> get_filename()
        'a1b2c3d4-a1b2-a1b2-a1b2-a1b2c3d4e5f6'
        >>> get_filename("2021-01-01.parquet")
        '2021-01-01'
        >>> get_filename("2021-01-01.snappy.parquet")
        '2021-01-01'
    """
    if filename is None:
        return f"{uuid.uuid4()}"
    else:
        filename.split(".")


def process_snapshot_data_file(
    df: pl.DataFrame,
    s3_client: "S3Client",
    extract_record_id: DerivedColumn,
    extract_create_time: DerivedColumn,
    extract_update_time: DerivedColumn,
    extract_partition_keys: T.List[DerivedColumn],
    s3dir_staging: S3Path,
    filename: T.Optional[str] = None,
    polars_write_parquet_kwargs: T_OPTIONAL_KWARGS = None,
    s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
    logger=dummy_logger,
) -> T.List[T_DATA_FILE]:
    """
    Convert the Snapshot Data File to many Staging Data Files, based on the
    number of partition keys.

    :param n_lines: if you want to read only the first n lines, then specify it.
        this is very useful for debugging.
    :param polars_dataframe_kwargs: custom keyword arguments for
        ``polars.DataFrame``. Default is ``None``.
    :param polars_write_parquet_kwargs: custom keyword arguments for
        ``polars.DataFrame.write_parquet``. Default is ``dict(compression="snappy")``.
    """
    # Derive more columns for data lake
    logger.info(
        "Derive record_id, create_time, update_time, and partition keys columns ..."
    )
    df = generate_more_columns(
        df,
        [
            extract_record_id,
            extract_create_time,
            extract_update_time,
            *extract_partition_keys,
        ],
    )

    # prepare variables for following operations
    if polars_write_parquet_kwargs is None:
        polars_write_parquet_kwargs = dict(compression="snappy")
    compression = polars_write_parquet_kwargs["compression"]
    filename = get_filename(filename) + f".{compression}.parquet"
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
        results = group_by_partition(
            df=df,
            s3dir=s3dir_staging,
            filename=filename,
            partition_keys=partition_keys,
            sort_by=[extract_update_time.alias],
        )
        logger.info(f"Will write data to {len(results)} partitions ...")
        for ith, (sub_df, s3path) in enumerate(results, start=1):
            logger.info(
                f"Write to {ith}th partition: {s3path.relative_to(s3dir_staging)}"
            )
            logger.info(f"  s3uri: {s3path.uri}")
            logger.info(f"  preview at: {s3path.console_url}")
            staging_data_file = {
                KeyEnum.URI: s3path.uri,
                KeyEnum.MD5: None,
                KeyEnum.N_RECORD: sub_df.shape[0],
            }
            size = write_data_file(
                df=sub_df,
                s3_client=s3_client,
                s3path=s3path,
                polars_write_parquet_kwargs=polars_write_parquet_kwargs,
                s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
            )
            staging_data_file[KeyEnum.SIZE] = size
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
        s3path = s3dir_staging.joinpath(filename)
        logger.info(f"Write to: {s3path.uri}")
        logger.info(f"  preview at: {s3path.console_url}")
        staging_data_file = {
            KeyEnum.URI: s3path.uri,
            KeyEnum.MD5: None,
            KeyEnum.N_RECORD: df.shape[0],
        }
        size = write_data_file(
            df=df,
            s3_client=s3_client,
            s3path=s3path,
            polars_write_parquet_kwargs=polars_write_parquet_kwargs,
            s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
        )
        staging_data_file[KeyEnum.SIZE] = size
        staging_data_file_list.append(staging_data_file)

    s3path_manifest = (
        s3dir_staging
        / DBSNAPLAKE_MANIFEST_FOLDER
        / f"manifest-data-{filename}.ndjson.gz"
    )
    s3path_manifest_summary = (
        s3dir_staging / DBSNAPLAKE_MANIFEST_FOLDER / f"manifest-summary-{filename}.json"
    )
    logger.info("Write generated files information to manifest file ...")
    logger.info(f"  Write to: {s3path_manifest_summary.uri} and {s3path_manifest.uri}")
    logger.info(
        f"  preview at: {s3path_manifest_summary.console_url} and {s3path_manifest.console_url}"
    )
    manifest_file = ManifestFile.new(
        uri=s3path_manifest.uri,
        uri_summary=s3path_manifest_summary.uri,
        data_file_list=staging_data_file_list,
        calculate=True,
    )
    manifest_file.write(s3_client=s3_client)
    return staging_data_file_list
