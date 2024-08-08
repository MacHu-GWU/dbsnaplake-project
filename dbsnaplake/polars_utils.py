# -*- coding: utf-8 -*-

"""

"""

import typing as T
import io

import polars as pl
from s3pathlib import S3Path

from .logger import dummy_logger
from .partition import encode_hive_partition

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def write_parquet_to_s3(
    df: pl.DataFrame,
    s3path: S3Path,
    s3_client: "S3Client",
    polars_write_parquet_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
    s3pathlib_write_bytes_kwargs: T.Optional[T.Dict[str, T.Any]] = None,
):
    """
    Write polars dataframe to AWS S3 as a parquet file.

    :param df: ``polars.DataFrame`` object.
    :param s3path: ``s3pathlib.S3Path`` object.
    :param s3_client: ``boto3.client("s3")`` object.
    :param polars_write_parquet_kwargs: Keyword arguments for
        ``polars.DataFrame.write_parquet`` method. See
        https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_parquet.html
    :param s3pathlib_write_bytes_kwargs: Keyword arguments for
        ``s3path.write_bytes`` method. See
        https://s3pathlib.readthedocs.io/en/latest/s3pathlib/core/rw.html#s3pathlib.core.rw.ReadAndWriteAPIMixin.write_bytes
    """
    if polars_write_parquet_kwargs is None:
        polars_write_parquet_kwargs = {}
    buffer = io.BytesIO()
    df.write_parquet(buffer, **polars_write_parquet_kwargs)

    if s3pathlib_write_bytes_kwargs is None:
        s3pathlib_write_bytes_kwargs = {}
    s3path.write_bytes(buffer.getvalue(), bsm=s3_client, **s3pathlib_write_bytes_kwargs)


def group_by_partition(
    df: pl.DataFrame,
    s3dir: S3Path,
    filename: str,
    partition_keys: T.List[str],
    sort_by: T.List[str],
) -> T.List[T.Tuple[pl.DataFrame, S3Path]]:
    """
    Group dataframe by partition keys and locate the S3 location for each partition.

    :param df: ``polars.DataFrame`` object.
    :param s3dir: ``s3pathlib.S3Path`` object, the root directory of the S3 location.
    :param filename: filename of the parquet file. for example: "data.parquet".
    :param partition_keys: list of partition keys. for example: ["year", "month"].
    :param sort_by: list of columns to sort by. for example: ["create_time"].
        use empty list if no sorting is needed.
    """
    results = list()
    partition_values: T.List[str]
    for ith, (partition_values, sub_df) in enumerate(
        df.group_by(partition_keys),
        start=1,
    ):
        if sort_by:
            sub_df = sub_df.sort(by=sort_by)
        kvs = dict(zip(partition_keys, partition_values))
        partition_relpath = encode_hive_partition(kvs=kvs)
        s3path = s3dir.joinpath(partition_relpath, filename)
        results.append((sub_df, s3path))
    return results
