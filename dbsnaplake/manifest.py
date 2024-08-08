# -*- coding: utf-8 -*-

"""
This module defines the abstraction of the transformation process from
Snapshot Data File to Staging Data File.
"""

import typing as T
import io
import gzip
import json
import uuid
import dataclasses

import polars as pl


try:
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pass
from s3pathlib import S3Path

from .typehint import T_EXTRACTOR, T_OPTIONAL_KWARGS
from .constants import (
    S3_METADATA_KEY_N_RECORDS,
)
from .logger import dummy_logger
from .compaction import File, FileGroup, calculate_merge_plan
from .polars_utils import write_parquet_to_s3, group_by_partition

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass(slots=True)
class SnapshotDataFile:
    """
    Represent a Snapshot Data File. For example,

    - AWS RDS Export to S3: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ExportSnapshot.html#USER_ExportSnapshot.FileNames
    - AWS DynamoDB Export to S3: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.Output.html
    """

    uri: str = dataclasses.field()
    size: T.Optional[int] = dataclasses.field(default=None)
    n_records: T.Optional[int] = dataclasses.field(default=None)

    @property
    def s3path(self) -> S3Path:
        return S3Path.from_s3_uri(self.uri)

    def to_dict(self) -> dict:
        return {
            "uri": self.uri,
            "size": self.size,
            "n_records": self.n_records,
        }

    @classmethod
    def from_dict(cls, dct: T.Dict[str, T.Any]):
        return cls(**dct)


T_SNAPSHOT_DATA_FILE = T.TypeVar("T_SNAPSHOT_DATA_FILE", bound=SnapshotDataFile)


@dataclasses.dataclass
class SnapshotManifestFile:
    """
    Manifest file is an uncompressed json line file. Each line is a
    :class:`SnapshotDataFile` object.
    """

    uri: str = dataclasses.field()
    snapshot_data_file_list: T.List[T_SNAPSHOT_DATA_FILE] = dataclasses.field(
        default_factory=list
    )

    @property
    def s3path(self) -> S3Path:
        return S3Path.from_s3_uri(self.uri)

    def write_v1(
        self,
        s3_client: "S3Client",
    ):
        """
        Write the manifest file to S3.
        """
        lines = [
            json.dumps(snapshot_data_file.to_dict())
            for snapshot_data_file in self.snapshot_data_file_list
        ]
        return self.s3path.write_bytes(
            gzip.compress("\n".join(lines).encode("utf-8")),
            content_type="application/json",
            content_encoding="gzip",
            bsm=s3_client,
        )

    def write_v2(
        self,
        s3_client: "S3Client",
    ):
        """
        Write the manifest file to S3.
        """
        df = pl.DataFrame(
            [
                snapshot_data_file.to_dict()
                for snapshot_data_file in self.snapshot_data_file_list
            ]
        )
        buffer = io.BytesIO()
        df.write_ndjson(buffer)
        return self.s3path.write_bytes(
            gzip.compress(buffer.getvalue()),
            content_type="application/json",
            content_encoding="gzip",
            bsm=s3_client,
        )

    write = write_v2

    @classmethod
    def read_v1(cls, uri: str, s3_client: "S3Client"):
        """
        Read the manifest file from S3.
        """
        s3path = S3Path.from_s3_uri(uri)
        lines = (
            gzip.decompress(s3path.read_bytes(bsm=s3_client))
            .decode("utf-8")
            .splitlines()
        )
        return cls(
            uri=uri,
            snapshot_data_file_list=[
                SnapshotDataFile.from_dict(json.loads(line))
                for line in gzip.decompress(s3path.read_bytes(bsm=s3_client))
                .decode("utf-8")
                .splitlines()
            ],
        )

    @classmethod
    def read_v2(cls, uri: str, s3_client: "S3Client"):
        """
        Read the manifest file from S3.
        """
        s3path = S3Path.from_s3_uri(uri)
        df = pl.read_ndjson(gzip.decompress(s3path.read_bytes(bsm=s3_client)))
        return cls(
            uri=uri,
            snapshot_data_file_list=[
                SnapshotDataFile.from_dict(record) for record in df.to_dicts()
            ],
        )

    read = read_v2

    def figure_out_target_size(self) -> int: # pragma: no cover
        # total_size = sum([
        #     snapshot_data_file.size
        #     for snapshot_data_file in self.snapshot_data_file_list
        # ])
        return 100 * 1000 * 1000  # 100 MB

    def group_files_into_tasks(
        self,
        target_size: int = 100 * 1000 * 1000, ## 100 MB
    ) -> T.List["SnapshotToStagingTask"]:
        """
        Group the snapshot data files into tasks.
        """
        mapping = {
            snapshot_data_file.uri: snapshot_data_file
            for snapshot_data_file in self.snapshot_data_file_list
        }

        files = [
            File(
                id=snapshot_data_file.uri,
                size=snapshot_data_file.size,
            )
            for snapshot_data_file in self.snapshot_data_file_list
        ]
        file_groups = calculate_merge_plan(target_size=target_size, files=files)
        return [
            SnapshotToStagingTask(
                id=str(uuid.uuid4()),
                snapshot_data_file_list=[mapping[file.id] for file in file_group.files],
            )
            for file_group in file_groups
        ]


@dataclasses.dataclass(slots=True)
class SnapshotToStagingTask:
    # fmt: off
    id: str = dataclasses.field()
    snapshot_data_file_list: T.List[SnapshotDataFile] = dataclasses.field(default_factory=list)
    # fmt: on
