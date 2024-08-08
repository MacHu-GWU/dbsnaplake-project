# -*- coding: utf-8 -*-

"""
In ETL (Extract, Transform, Load) pipelines, it's a common practice to group
numerous files into appropriately sized batches, each forming a distinct task.
This approach optimizes processing efficiency and resource utilization.

However, this method reqiures an effective mechanism for storing and
retrieving metadata. Ideally, we should be able to access the metadata for
an entire task in a single operation, eliminating the need to read each file
individually. This approach significantly reduces I/O operations and improves
overall performance.

This module implements an abstraction layer to achieve this functionality.
It provides a streamlined interface for grouping files, managing their associated
metadata, and enabling efficient batch processing in ETL workflows.
"""

import typing as T
import io
import gzip
import json
import dataclasses

import polars as pl


try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pass
from s3pathlib import S3Path

from .typehint import T_RECORD, T_OPTIONAL_KWARGS
from .constants import (
    S3_METADATA_KEY_SIZE,
    S3_METADATA_KEY_N_RECORD,
)
from .compaction import calculate_merge_plan
from .polars_utils import write_parquet_to_s3

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def write_ndjson_v1(records: T.List[T_RECORD]) -> bytes:
    lines = [json.dumps(record) for record in records]
    return gzip.compress("\n".join(lines).encode("utf-8"))


def write_ndjson_v2(records: T.List[T_RECORD]) -> bytes:
    df = pl.DataFrame(records)
    buffer = io.BytesIO()
    df.write_ndjson(buffer)
    return gzip.compress(buffer.getvalue())


def read_ndjson_v1(b: bytes) -> T.List[T_RECORD]:
    lines = gzip.decompress(b).decode("utf-8").splitlines()
    return [json.loads(line) for line in lines]


def read_ndjson_v2(b: bytes) -> T.List[T_RECORD]:
    df = pl.read_ndjson(gzip.decompress(b))
    return df.to_dicts()


read_ndjson = read_ndjson_v2
write_ndjson = write_ndjson_v2


@dataclasses.dataclass(slots=True)
class DataFile:
    """
    A file is a unit of data that can be processed independently.
    """

    uri: str = dataclasses.field()
    size: T.Optional[int] = dataclasses.field(default=None)
    n_record: T.Optional[int] = dataclasses.field(default=None)

    @property
    def s3path(self) -> S3Path:
        return S3Path.from_s3_uri(self.uri)

    def to_dict(self) -> dict:
        return {
            "uri": self.uri,
            "size": self.size,
            "n_record": self.n_record,
        }

    @classmethod
    def from_dict(cls, dct: T.Dict[str, T.Any]):
        return cls(**dct)

    def write_parquet(
        self,
        df: pl.DataFrame,
        s3_client: "S3Client",
        polars_write_parquet_kwargs: T_OPTIONAL_KWARGS = None,
        s3pathlib_write_bytes_kwargs: T_OPTIONAL_KWARGS = None,
    ):
        """
        Write the DataFrame to the given S3Path as a Parquet file, also attach
        additional information related to the Snapshot Data File.

        It is a wrapper of the write_parquet_to_s3 function, make the final code shorter.
        """
        if s3pathlib_write_bytes_kwargs is None:
            s3pathlib_write_bytes_kwargs = {}
        s3pathlib_write_bytes_kwargs["content_type"] = "application/x-parquet"
        more_metadata = {
            S3_METADATA_KEY_N_RECORD: str(df.shape[0]),
        }
        if "metadata" in s3pathlib_write_bytes_kwargs:
            s3pathlib_write_bytes_kwargs["metadata"].update(more_metadata)
        else:
            s3pathlib_write_bytes_kwargs["metadata"] = more_metadata
        return write_parquet_to_s3(
            df=df,
            s3path=self.s3path,
            s3_client=s3_client,
            polars_write_parquet_kwargs=polars_write_parquet_kwargs,
            s3pathlib_write_bytes_kwargs=s3pathlib_write_bytes_kwargs,
        )


T_DATA_FILE = T.TypeVar("T_DATA_FILE", bound=DataFile)


@dataclasses.dataclass(slots=True)
class ManifestFile:
    uri: str = dataclasses.field()
    size: T.Optional[int] = dataclasses.field(default=None)
    n_record: T.Optional[int] = dataclasses.field(default=None)
    data_file_list: T.List[T_DATA_FILE] = dataclasses.field(default_factory=list)

    @property
    def s3path(self) -> S3Path:
        return S3Path.from_s3_uri(self.uri)

    def write(self, s3_client: "S3Client"):
        """
        Write the manifest file to S3.
        """
        records = list()
        size_list = list()
        n_record_list = list()
        for data_file in self.data_file_list:
            records.append(data_file.to_dict())
            size_list.append(data_file.size)
            n_record_list.append(data_file.n_record)
        metadata = {}
        try:
            size = sum(size_list)
            self.size = size
            metadata[S3_METADATA_KEY_SIZE] = str(size)
        except:
            pass
        try:
            n_record = sum(n_record_list)
            self.n_record = n_record
            metadata[S3_METADATA_KEY_N_RECORD] = str(n_record)
        except:
            pass
        return self.s3path.write_bytes(
            write_ndjson(records),
            content_type="application/json",
            content_encoding="gzip",
            metadata=metadata,
            bsm=s3_client,
        )

    @classmethod
    def read(cls, uri: str, s3_client: "S3Client"):
        """
        Read the manifest file from S3.
        """
        s3path = S3Path.from_s3_uri(uri)
        records = read_ndjson((s3path.read_bytes(bsm=s3_client)))
        try:
            size = int(s3path.metadata.get(S3_METADATA_KEY_SIZE))
        except:
            size = None
        try:
            n_record = int(s3path.metadata.get(S3_METADATA_KEY_N_RECORD))
        except:
            n_record = None
        return cls(
            uri=uri,
            size=size,
            n_record=n_record,
            data_file_list=[DataFile.from_dict(record) for record in records],
        )

    def group_files_into_tasks(
        self,
        target_size: int = 100 * 1000 * 1000,  ## 100 MB
    ) -> T.List[T.List["T_DATA_FILE"]]:
        """
        Group the snapshot data files into tasks.
        """
        mapping = {data_file.uri: data_file for data_file in self.data_file_list}
        files = [(data_file.uri, data_file.size) for data_file in self.data_file_list]
        file_groups = calculate_merge_plan(files=files, target=target_size)
        data_file_group_list = list()
        for file_group in file_groups:
            data_file_list = [
                mapping[uri] for uri, size in file_group
            ]
            data_file_group_list.append(data_file_list)
        return data_file_group_list


T_MANIFEST_FILE = T.TypeVar("T_MANIFEST_FILE", bound=ManifestFile)
