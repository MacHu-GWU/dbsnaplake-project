# -*- coding: utf-8 -*-

"""
Datalake partition utilities.

This module provides functions and classes for managing and manipulating
partitions in a data lake stored on S3. It includes utilities for extracting
partition information, encoding partition data, and listing partitions.
"""

import typing as T
import dataclasses

from s3pathlib import S3Path

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class Partition:
    """
    Represents a partition in an S3-based data lake.

    A partition is a directory in S3 that contains data files but no subdirectories.
    It typically follows a hierarchical structure based on partition keys.

    For example, in the following S3 directory structure::

        s3://bucket/folder/year=2021/month=01/day=01/data.json
        s3://bucket/folder/year=2021/month=01/day=02/data.json
        s3://bucket/folder/year=2021/month=02/day=01/data.json
        s3://bucket/folder/year=2021/month=02/day=02/data.json

    Then:

    - ``s3://bucket/folder/year=2021/month=01/day=01/`` is a partition.
    - ``s3://bucket/folder/year=2021/month=01/`` is NOT a partition.
    - ``s3://bucket/folder/year=2021/`` is NOT a partition.

    :param uri: The S3 URI of the partition. For example:
        ``s3://bucket/folder/year=2021/month=01/day=01/data.json``
    :param data: A dictionary of partition data. Note that the value is always
        a string, even if it represents a number.
        For example: ``{"year": "2021", "month": "01", "day": "01"}``
    """

    uri: str = dataclasses.field()
    data: T.Dict[str, str] = dataclasses.field()

    def __post_init__(self):
        if self.uri.endswith("/") is False:
            self.uri = self.uri + "/"

    @property
    def s3dir(self) -> S3Path:
        """
        The S3 directory path of the partition.
        """
        return S3Path.from_s3_uri(self.uri)

    @classmethod
    def from_uri(
        cls,
        s3uri: str,
        s3uri_root: str,
    ):
        """
        Construct a Partition object from S3 URIs.

        :param s3uri: The S3 URI of the partition.
        :param s3uri_root: The S3 URI of the root directory.

        :return: A new :class:`Partition` object.
        """
        s3dir = S3Path.from_s3_uri(s3uri).to_dir()
        s3dir_root = S3Path.from_s3_uri(s3uri_root).to_dir()
        data = extract_partition_data(s3dir_root, s3dir)
        return cls(uri=s3uri, data=data)

    def list_parquet_files(
        self,
        s3_client: "S3Client",
        ext: str = ".parquet",
    ) -> T.List[S3Path]:  # pragma: no cover
        """
        List all Parquet files in the partition.

        :param ext: File extension to filter. Defaults to ".parquet".

        :return: A list of S3Path objects representing Parquet files.
        """
        return (
            self.s3dir.iter_objects(bsm=s3_client)
            .filter(lambda x: x.basename.endswith(ext))
            .all()
        )


def extract_partition_data(
    s3dir_root: S3Path,
    s3dir_partition: S3Path,
) -> T.Dict[str, str]:
    """
    Extract partition data from the S3 directory path.

    :param s3dir_root: The root S3 directory.
    :param s3dir_partition: The partition S3 directory.

    **Example**

    >>> s3dir_root = S3Path("s3://bucket/folder/")
    >>> s3dir_partition = S3Path("s3://bucket/folder/year=2021/month=01/day=15/")
    >>> extract_partition_data(s3dir_root, s3dir_partition)
    {"year": "2021", "month": "01", "day": "15"}
    """
    data = dict()
    for part in s3dir_partition.relative_to(s3dir_root).parts:
        key, value = part.split("=", 1)
        data[key] = value
    return data


def encode_hive_partition(kvs: T.Dict[str, str]) -> str:
    """
    Encode partition data into hive styled partition string.

    :param kvs: A dictionary of partition key-value pairs.

    For example:

        >>> encode_hive_partition({"year": "2021", "month": "01", "day": "01"})
        'year=2021/month=01/day=01'
    """
    return "/".join([f"{k}={v}" for k, v in kvs.items()])


def get_s3dir_partition(
    s3dir_root: S3Path,
    kvs: T.Dict[str, str],
) -> S3Path:
    """
    Get the S3 directory path of the partition.

    :param s3dir_root: The root S3 directory.
    :param kvs: A dictionary of partition key-value pairs.

    **Example**

    >>> s3dir_root = S3Path("s3://bucket/folder/")
    >>> s3dir_partition = get_s3dir_partition(s3dir_root, {"year": "2021", "month": "01", "day": "01"})
    >>> s3dir_partition.uri
    's3://bucket/folder/year=2021/month=01/day=01/'
    """
    return (s3dir_root / encode_hive_partition(kvs)).to_dir()


def get_partitions_v2(
    s3_client: "S3Client",
    s3dir_root: S3Path,
    _s3dir_partition: T.Optional[S3Path] = None,
    _partitions: T.List[Partition] = None,
) -> T.List[Partition]:  # pragma: no cover
    """
    Recursively scan the S3 directory and return a list of partitions.

    For example, for the following S3 structure:

        s3://bucket/folder/year=2021/month=01/day=01/data.json
        s3://bucket/folder/year=2021/month=01/day=02/data.json
        s3://bucket/folder/year=2021/month=02/day=01/data.json
        s3://bucket/folder/year=2021/month=02/day=02/data.json

    The function will return partitions::

        s3://bucket/folder/year=2021/month=01/day=01/
        s3://bucket/folder/year=2021/month=01/day=02/
        s3://bucket/folder/year=2021/month=02/day=01/
        s3://bucket/folder/year=2021/month=02/day=02/

    .. note::

        This implementation recursively scan all S3 folder. It is slower
        than :func:`get_partitions`. I intentionally leave this code here
        for reference.
    """
    if _partitions is None:
        _partitions = []
        s3dir_iter_from = s3dir_root
    else:
        s3dir_iter_from = _s3dir_partition

    s3path_list = list()
    s3dir_list = list()
    for s3path in s3dir_iter_from.iterdir():
        if s3path.is_dir():
            s3dir_list.append(s3path)
        else:
            s3path_list.append(s3path)

    if len(s3path_list) == 0:
        try:
            _partitions.pop()
        except IndexError:
            pass

    for s3dir in s3dir_list:
        data = extract_partition_data(s3dir_root, s3dir)
        partition = Partition(uri=s3dir.uri, data=data)
        _partitions.append(partition)
        get_partitions(
            s3_client=s3_client,
            s3dir_root=s3dir_root,
            _s3dir_partition=s3dir,
            _partitions=_partitions,
        )

    return _partitions


def get_partitions(
    s3_client: "S3Client",
    s3dir_root: S3Path,
) -> T.List[Partition]:
    """
    Efficiently scan the S3 directory and return a list of partitions.

    For example, for the following S3 structure:

        s3://bucket/folder/year=2021/month=01/day=01/data.json
        s3://bucket/folder/year=2021/month=01/day=02/data.json
        s3://bucket/folder/year=2021/month=02/day=01/data.json
        s3://bucket/folder/year=2021/month=02/day=02/data.json

    The function will return partitions::

        s3://bucket/folder/year=2021/month=01/day=01/
        s3://bucket/folder/year=2021/month=01/day=02/
        s3://bucket/folder/year=2021/month=02/day=01/
        s3://bucket/folder/year=2021/month=02/day=02/

    .. note::

        This implementation has higher performance compared to
        :func:`get_partitions_v1` as it avoids recursive S3 API calls.
    """
    # locate all s3 folder that has file in it
    s3_uri_set = {
        s3path.parent.uri for s3path in s3dir_root.iter_objects(bsm=s3_client)
    }
    s3_uri_list = list()
    # make sure either it is the s3dir_root or it has "=" character in it
    len_s3dir_root = len(s3dir_root.uri)
    for s3_uri in s3_uri_set:
        # sometimes we may have non partition folder, such as ``.hoodie`` folder
        # so we should check if there's a "=" character in it.
        if ("=" in s3_uri.split("/")[-2]) or (len(s3_uri) == len_s3dir_root):
            s3_uri_list.append(s3_uri)
    # convert partition uri list to partition object list
    s3_uri_list.sort()
    partition_list = list()
    for s3_uri in s3_uri_list:
        s3dir = S3Path.from_s3_uri(s3_uri)
        data = extract_partition_data(s3dir_root, s3dir)
        partition = Partition(uri=s3dir.uri, data=data)
        partition_list.append(partition)
    return partition_list
