# -*- coding: utf-8 -*-

import math
from datetime import datetime

import numpy as np
import polars as pl
from s3pathlib import S3Path
from s3manifesto.api import KeyEnum

from dbsnaplake._import_utils import write_parquet_to_s3
from dbsnaplake._import_utils import DBSnapshotManifestFile


def generate_db_snapshot_file_data(
    s3_client,
    s3dir_snapshot: S3Path,
    s3path_db_snapshot_manifest_summary: S3Path,
    s3path_db_snapshot_manifest_data: S3Path,
    n_db_snapshot_file: int,
    n_db_snapshot_record: int,
) -> DBSnapshotManifestFile:
    """
    Generate test db snapshot data and manifest file for unit test.
    """
    epoch = datetime(1970, 1, 1)
    start_time = datetime(2021, 1, 1)
    end_time = datetime(2021, 12, 31, 23, 59, 59)
    start_ts = int((start_time - epoch).total_seconds())
    end_ts = int((end_time - epoch).total_seconds())
    df = pl.DataFrame(
        {
            "order_id": range(1, 1 + n_db_snapshot_record),
            "order_time": np.random.randint(start_ts, end_ts + 1, n_db_snapshot_record),
            "amount": np.random.rand(n_db_snapshot_record) * 1000,
        }
    )
    df = df.with_columns(
        pl.concat_str([pl.lit("order-"), pl.col("order_id").cast(pl.Utf8)]).alias(
            "order_id"
        ),
        pl.from_epoch(pl.col("order_time")).alias("order_time"),
    )
    n_record_per_file = math.ceil(n_db_snapshot_record // n_db_snapshot_file)
    data_file_list = list()
    for ith, sub_df in enumerate(
        df.iter_slices(n_rows=n_record_per_file),
        start=1,
    ):
        s3path = s3dir_snapshot.joinpath(f"{ith}.parquet")
        size, n_record, etag = write_parquet_to_s3(
            df=sub_df,
            s3path=s3path,
            s3_client=s3_client,
        )
        data_file = {
            KeyEnum.URI: s3path.uri,
            KeyEnum.ETAG: etag,
            KeyEnum.SIZE: size,
            KeyEnum.N_RECORD: n_record,
        }
        data_file_list.append(data_file)
    db_snapshot_manifest_file = DBSnapshotManifestFile.new(
        uri=s3path_db_snapshot_manifest_data.uri,
        uri_summary=s3path_db_snapshot_manifest_summary.uri,
        data_file_list=data_file_list,
        details={"create_by": "dbsnaplake"},
        calculate=True,
    )
    db_snapshot_manifest_file.write(s3_client=s3_client)
    return db_snapshot_manifest_file
