# -*- coding: utf-8 -*-

"""
Polars utilities.
"""

import typing as T
import io
import gzip

import polars as pl
from s3pathlib import S3Path
from polars_writer.writer import Writer
from .typehint import T_OPTIONAL_KWARGS
from .constants import S3_METADATA_KEY_N_RECORD, S3_METADATA_KEY_N_COLUMN
from .partition import encode_hive_partition

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def configure_s3_write_options(
    df: pl.DataFrame,
    polars_writer: Writer,
    gzip_compress: bool,
    s3pathlib_write_bytes_kwargs: T.Dict[str, T.Any],
) -> str:
    """
    Configure S3 write options based on the polars writer.

    This function sets up the necessary metadata and content-related parameters for
    writing a Polars DataFrame to S3. It determines the appropriate file extension
    and configures compression settings based on the writer format and user preferences.

    :param df: The Polars DataFrame to be written.
    :param polars_writer: The Polars writer object specifying the output format.
    :param gzip_compress: Whether to apply gzip compression (where applicable).
    :param s3pathlib_write_bytes_kwargs: Dictionary of keyword arguments
        for S3 write operation, to be modified in-place.

    :return: The appropriate file extension for the configured write operation.
    """
    more_metadata = {
        S3_METADATA_KEY_N_RECORD: str(df.shape[0]),
        S3_METADATA_KEY_N_COLUMN: str(df.shape[1]),
    }
    if "metadata" in s3pathlib_write_bytes_kwargs:
        s3pathlib_write_bytes_kwargs["metadata"].update(more_metadata)
    else:
        s3pathlib_write_bytes_kwargs["metadata"] = more_metadata

    if polars_writer.is_csv():
        s3pathlib_write_bytes_kwargs["content_type"] = "text/plain"
        if gzip_compress:
            s3pathlib_write_bytes_kwargs["content_encoding"] = "gzip"
            return ".csv.gzip"
        else:
            return ".csv"
    elif polars_writer.is_json() or polars_writer.is_ndjson():
        s3pathlib_write_bytes_kwargs["content_type"] = "application/json"
        if gzip_compress:
            s3pathlib_write_bytes_kwargs["content_encoding"] = "gzip"
            return ".json.gzip"
        else:
            return ".json"
    elif polars_writer.is_parquet():
        s3pathlib_write_bytes_kwargs["content_type"] = "application/x-parquet"
        if isinstance(polars_writer.parquet_compression, str):
            if gzip_compress is True:
                raise ValueError(
                    "For Parquet, gzip_compress must be False. "
                    "You should use Writer.parquet_compression to specify the compression."
                )
            s3pathlib_write_bytes_kwargs["content_encoding"] = (
                polars_writer.parquet_compression
            )
            return f".{polars_writer.parquet_compression}.parquet"
        else:
            # use snappy as the default compression
            polars_writer.parquet_compression = "snappy"
            if gzip_compress is True:
                polars_writer.parquet_compression = "gzip"
                s3pathlib_write_bytes_kwargs["content_encoding"] = "gzip"
                return ".gzip.parquet"
            else:
                s3pathlib_write_bytes_kwargs["content_encoding"] = "snappy"
                return ".snappy.parquet"
    elif polars_writer.is_delta():  # pragma: no cover
        raise NotImplementedError
    else:  # pragma: no cover
        raise ValueError(f"Unsupported format: {polars_writer.format}")


def configure_s3path(
    s3dir: T.Optional[S3Path] = None,
    fname: T.Optional[str] = None,
    ext: T.Optional[str] = None,
    s3path: T.Optional[S3Path] = None,
):
    """
    Configure and return an S3Path object for file operations.

    This function allows flexible specification of an S3 path. It can either construct
    a path from individual components (directory, filename, and extension) or use a
    pre-configured S3Path object.

    :param s3dir: The S3 directory path. Required if s3path is not provided.
    :param fname: The filename without extension. Required if s3path is not provided.
        for example, if the full file name is "data.csv", then fname is "data".
    :param ext: The file extension, including the dot (e.g., '.csv').
        Required if s3path is not provided.
    :param s3path: A pre-configured S3Path object. If provided, other arguments are ignored.

    :return The configured S3Path object representing the full file path in S3.
    """
    if s3path is None:
        if (s3dir is None) or (fname is None) or (ext is None):
            raise ValueError(
                "s3dir, fname, and ext must be provided when s3path is not provided"
            )
        return s3dir.joinpath(fname + ext)
    else:
        return s3path


def write_to_s3(
    df: pl.DataFrame,
    s3_client: "S3Client",
    polars_writer: Writer,
    gzip_compress: bool = False,
    s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
    s3dir: T.Optional[S3Path] = None,
    fname: T.Optional[str] = None,
    s3path: T.Optional[S3Path] = None,
) -> T.Tuple[S3Path, T.Optional[int], T.Optional[str]]:
    """
    Write the DataFrame to the given S3Path object, also attach
    additional information related to the dataframe.

    The original ``polars.write_parquet`` method doesn't work with moto,
    so we use buffer to store the parquet file and then write it to S3.

    :param df: ``polars.DataFrame`` object.
    :param s3_client: ``boto3.client("s3")`` object.
    :param polars_writer: `polars_writer.api.Writer <https://github.com/MacHu-GWU/polars_writer-project>`_
        object.
    :param gzip_compress: Flag to enable GZIP compression.
    :param s3pathlib_write_bytes_kwargs: Keyword arguments for
        ``s3path.write_bytes`` method. See
        https://s3pathlib.readthedocs.io/en/latest/s3pathlib/core/rw.html#s3pathlib.core.rw.ReadAndWriteAPIMixin.write_bytes
    :param s3dir: The S3 directory path. Required if s3path is not provided.
    :param fname: The filename without extension. Required if s3path is not provided.
        for example, if the full file name is "data.csv", then fname is "data".
    :param s3path: A pre-configured S3Path object. If provided, other arguments are ignored.

    :return: A tuple of three values:
        - The S3Path object representing the full file path in S3.
        - The number of bytes written to S3, i.e., the size of the parquet file.
        - The ETag of the S3 object.
    """
    if s3pathlib_write_bytes_kwargs is None:
        s3pathlib_write_bytes_kwargs = {}
    if (
        polars_writer.is_csv()
        or polars_writer.is_json()
        or polars_writer.is_ndjson()
        or polars_writer.is_parquet()
    ):
        buffer = io.BytesIO()
        polars_writer.write(df, file_args=[buffer])
        b = buffer.getvalue()
        if (polars_writer.is_parquet() is False) and gzip_compress:
            b = gzip.compress(b)
        ext = configure_s3_write_options(
            df=df,
            polars_writer=polars_writer,
            gzip_compress=gzip_compress,
            s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
        )
        s3path = configure_s3path(
            s3dir=s3dir,
            fname=fname,
            ext=ext,
            s3path=s3path,
        )
        s3path_new = s3path.write_bytes(
            b, bsm=s3_client, **s3pathlib_write_bytes_kwargs
        )
        size = len(b)
        etag = s3path_new.etag
        return (s3path_new, size, etag)
    else:
        if s3dir is None:
            raise ValueError("s3dir must be provided for deltalake formats")
        polars_writer.write(df, file_args=[s3dir.uri])
        return (s3dir, None, None)


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
