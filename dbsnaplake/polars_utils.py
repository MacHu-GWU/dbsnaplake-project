# -*- coding: utf-8 -*-

"""
Polars utilities.
"""

import typing as T
import io

import polars as pl
from s3pathlib import S3Path
from polars_writer.writer import Writer
from .typehint import T_OPTIONAL_KWARGS
from .constants import S3_METADATA_KEY_N_RECORD, S3_METADATA_KEY_N_COLUMN
from .partition import encode_hive_partition

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def write_to_s3(
    df: pl.DataFrame,
    s3path: S3Path,
    s3_client: "S3Client",
    polars_writer: Writer,
    s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
) -> T.Tuple[int, str]:
    """
    Write the DataFrame to the given S3Path object, also attach
    additional information related to the dataframe.

    :param df: ``polars.DataFrame`` object.
    :param s3path: ``s3pathlib.S3Path`` object.
    :param s3_client: ``boto3.client("s3")`` object.
    :param polars_writer: `polars_writer.api.Writer <https://github.com/MacHu-GWU/polars_writer-project>`_
        object.
    :param s3pathlib_write_bytes_kwargs: Keyword arguments for
        ``s3path.write_bytes`` method. See
        https://s3pathlib.readthedocs.io/en/latest/s3pathlib/core/rw.html#s3pathlib.core.rw.ReadAndWriteAPIMixin.write_bytes

    :return: A tuple of two values:
        - The number of bytes written to S3, i.e., the size of the parquet file.
        - The ETag of the S3 object.
    """
    if s3pathlib_write_bytes_kwargs is None:
        s3pathlib_write_bytes_kwargs = {}
    more_metadata = {
        S3_METADATA_KEY_N_RECORD: str(df.shape[0]),
        S3_METADATA_KEY_N_COLUMN: str(df.shape[1]),
    }
    if "metadata" in s3pathlib_write_bytes_kwargs:
        s3pathlib_write_bytes_kwargs["metadata"].update(more_metadata)
    else:
        s3pathlib_write_bytes_kwargs["metadata"] = more_metadata
    buffer = io.BytesIO()
    polars_writer.write(df, file_args=[buffer])
    b = buffer.getvalue()
    s3path_new = s3path.write_bytes(b, bsm=s3_client, **s3pathlib_write_bytes_kwargs)
    size = len(b)
    etag = s3path_new.etag
    return (size, etag)


def write_parquet_to_s3(
    df: pl.DataFrame,
    s3path: S3Path,
    s3_client: "S3Client",
    polars_write_parquet_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
    s3pathlib_write_bytes_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
) -> T.Tuple[int, str]:
    """
    Write polars dataframe to AWS S3 as a parquet file.

    The original ``polars.write_parquet`` method doesn't work with moto.

    :param df: ``polars.DataFrame`` object.
    :param s3path: ``s3pathlib.S3Path`` object.
    :param s3_client: ``boto3.client("s3")`` object.
    :param polars_write_parquet_kwargs: Keyword arguments for
        ``polars.DataFrame.write_parquet`` method. See
        https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_parquet.html
    :param s3pathlib_write_bytes_kwargs: Keyword arguments for
        ``s3path.write_bytes`` method. See
        https://s3pathlib.readthedocs.io/en/latest/s3pathlib/core/rw.html#s3pathlib.core.rw.ReadAndWriteAPIMixin.write_bytes

    :return: A tuple of three values:
        - The number of bytes written to S3, i.e., the size of the parquet file.
        - The ETag of the S3 object.
    """
    if polars_write_parquet_kwargs is None:
        polars_write_parquet_kwargs = {}
    if s3pathlib_write_bytes_kwargs is None:
        s3pathlib_write_bytes_kwargs = {}
    more_metadata = {
        S3_METADATA_KEY_N_RECORD: str(df.shape[0]),
        S3_METADATA_KEY_N_COLUMN: str(df.shape[1]),
    }
    if "metadata" in s3pathlib_write_bytes_kwargs:
        s3pathlib_write_bytes_kwargs["metadata"].update(more_metadata)
    else:
        s3pathlib_write_bytes_kwargs["metadata"] = more_metadata
    s3pathlib_write_bytes_kwargs["content_type"] = "application/x-parquet"

    buffer = io.BytesIO()
    df.write_parquet(buffer, **polars_write_parquet_kwargs)
    b = buffer.getvalue()
    s3path_new = s3path.write_bytes(b, bsm=s3_client, **s3pathlib_write_bytes_kwargs)
    size = len(b)
    etag = s3path_new.etag
    return (size, etag)


def read_parquet_from_s3(
    s3path: S3Path,
    s3_client: "S3Client",
    polars_read_parquet_kwargs: T_OPTIONAL_KWARGS = None,
    s3pathlib_read_bytes_kwargs: T_OPTIONAL_KWARGS = None,
) -> pl.DataFrame:
    """
    Read parquet file from S3.

    :param s3path: ``s3pathlib.S3Path`` object.
    :param s3_client: ``boto3.client("s3")`` object.
    :param polars_read_parquet_kwargs: Keyword arguments for
        ``polars.read_parquet`` method. See
        https://docs.pola.rs/api/python/stable/reference/api/polars.read_parquet.html
    :param s3pathlib_read_bytes_kwargs: Keyword arguments for
        ``s3path.read_bytes`` method. See
        https://s3pathlib.readthedocs.io/en/latest/s3pathlib/core/rw.html#s3pathlib.core.rw.ReadAndWriteAPIMixin.read_bytes

    :return: ``polars.DataFrame`` object.
    """
    if polars_read_parquet_kwargs is None:
        polars_read_parquet_kwargs = {}
    if s3pathlib_read_bytes_kwargs is None:
        s3pathlib_read_bytes_kwargs = {}
    b = s3path.read_bytes(bsm=s3_client, **s3pathlib_read_bytes_kwargs)
    df = pl.read_parquet(b, **polars_read_parquet_kwargs)
    return df


def read_many_parquet_from_s3(
    s3path_list: T.List[S3Path],
    s3_client: "S3Client",
    polars_read_parquet_kwargs: T_OPTIONAL_KWARGS = None,
    s3pathlib_read_bytes_kwargs: T_OPTIONAL_KWARGS = None,
) -> pl.DataFrame:
    """
    Read many parquet files from S3 and concatenate them.

    :param s3path_list: list of ``s3pathlib.S3Path`` object.
    :param s3_client: ``boto3.client("s3")`` object.
    :param polars_read_parquet_kwargs: Keyword arguments for
        ``polars.read_parquet`` method. See
        https://docs.pola.rs/api/python/stable/reference/api/polars.read_parquet.html
    :param s3pathlib_read_bytes_kwargs: Keyword arguments for
        ``s3path.read_bytes`` method. See
        https://s3pathlib.readthedocs.io/en/latest/s3pathlib/core/rw.html#s3pathlib.core.rw.ReadAndWriteAPIMixin.read_bytes

    :return: ``polars.DataFrame`` object.
    """
    sub_df_list = list()
    for s3path in s3path_list:
        sub_df = read_parquet_from_s3(
            s3path=s3path,
            s3_client=s3_client,
            polars_read_parquet_kwargs=polars_read_parquet_kwargs,
            s3pathlib_read_bytes_kwargs=s3pathlib_read_bytes_kwargs,
        )
        sub_df_list.append(sub_df)
    df = pl.concat(sub_df_list)
    return df


def group_by_partition(
    df: pl.DataFrame,
    s3dir: S3Path,
    filename: str,
    partition_keys: T.List[str],
    sort_by: T.Optional[T.List[str]] = None,
    descending: T.Union[bool, T.List[bool]] = False,
) -> T.List[T.Tuple[pl.DataFrame, S3Path]]:
    """
    Group dataframe by partition keys and locate the S3 location for each partition.

    :param df: ``polars.DataFrame`` object.
    :param s3dir: ``s3pathlib.S3Path`` object, the root directory of the S3 location.
    :param filename: filename of the parquet file. for example: "data.parquet".
    :param partition_keys: list of partition keys. for example: ["year", "month"].
    :param sort_by: list of columns to sort by. for example: ["create_time"].
        use empty list or None if no sorting is needed.
    :param descending: list of boolean values to indicate the sorting order.
        for example: [True] or [False, True].
    """
    results = list()
    partition_values: T.List[str]
    for ith, (partition_values, sub_df) in enumerate(
        df.group_by(partition_keys),
        start=1,
    ):
        sub_df = sub_df.drop(partition_keys)
        if sort_by:
            sub_df = sub_df.sort(by=sort_by, descending=descending)
        kvs = dict(zip(partition_keys, partition_values))
        partition_relpath = encode_hive_partition(kvs=kvs)
        s3path = s3dir.joinpath(partition_relpath, filename)
        results.append((sub_df, s3path))
    return results
