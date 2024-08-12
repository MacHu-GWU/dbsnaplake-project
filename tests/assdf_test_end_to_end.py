# -*- coding: utf-8 -*-

import math
from datetime import datetime

import numpy as np
import polars as pl
from vislog import VisLog
from s3pathlib import S3Path, context
from s3manifesto.api import KeyEnum

from dbsnaplake._import_utils import T_RECORD
from dbsnaplake._import_utils import T_DF_SCHEMA
from dbsnaplake._import_utils import T_EXTRACTOR
from dbsnaplake._import_utils import T_OPTIONAL_KWARGS
from dbsnaplake._import_utils import repr_data_size
from dbsnaplake._import_utils import S3Location
from dbsnaplake._import_utils import Partition
from dbsnaplake._import_utils import extract_partition_data
from dbsnaplake._import_utils import encode_hive_partition
from dbsnaplake._import_utils import get_s3dir_partition
from dbsnaplake._import_utils import get_partitions
from dbsnaplake._import_utils import write_parquet_to_s3
from dbsnaplake._import_utils import write_data_file
from dbsnaplake._import_utils import read_parquet_from_s3
from dbsnaplake._import_utils import read_many_parquet_from_s3
from dbsnaplake._import_utils import group_by_partition
from dbsnaplake._import_utils import get_merged_schema
from dbsnaplake._import_utils import harmonize_schemas
from dbsnaplake._import_utils import dummy_logger
from dbsnaplake._import_utils import DBSnapshotManifestFile
from dbsnaplake._import_utils import DBSnapshotFileGroupManifestFile
from dbsnaplake._import_utils import batch_read_snapshot_data_file
from dbsnaplake._import_utils import DerivedColumn
from dbsnaplake._import_utils import StagingFileGroupManifestFile
from dbsnaplake._import_utils import process_db_snapshot_file_group_manifest_file
from dbsnaplake._import_utils import extract_s3dir
from dbsnaplake._import_utils import PartitionFileGroupManifestFile
from dbsnaplake._import_utils import execute_compaction
from dbsnaplake._import_utils import step_1_1_plan_snapshot_to_staging
from dbsnaplake._import_utils import step_1_2_get_snapshot_to_staging_todo_list
from dbsnaplake._import_utils import (
    step_1_3_process_db_snapshot_file_group_manifest_file,
)
from dbsnaplake._import_utils import step_2_1_plan_staging_to_datalake
from dbsnaplake._import_utils import step_2_2_get_staging_to_datalake_todo_list
from dbsnaplake._import_utils import step_2_3_process_partition_file_group_manifest_file
from dbsnaplake.tests.mock_aws import BaseMockAwsTest

logger = VisLog(name="test_end_to_end", log_format="%(message)s")


def prepare_db_snapshot_file_data(
    s3_client,
    s3dir_snapshot: S3Path,
    s3path_db_snapshot_manifest_file_summary: S3Path,
    s3path_db_snapshot_manifest_file_data: S3Path,
    n_db_snapshot_file: int,
    n_db_snapshot_record: int,
):
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
        uri=s3path_db_snapshot_manifest_file_data.uri,
        uri_summary=s3path_db_snapshot_manifest_file_summary.uri,
        data_file_list=data_file_list,
        details={"create_by": "test_end_to_end"},
        calculate=True,
    )
    db_snapshot_manifest_file.write(s3_client=s3_client)


class Test(BaseMockAwsTest):
    use_mock: bool = True
    s3_loc: S3Location = None
    n_db_snapshot_file: int = None
    n_db_snapshot_record: int = None
    n_record_per_file: int = None
    db_snapshot_manifest_file: DBSnapshotManifestFile = None
    target_db_snapshot_file_group_size: int = None
    target_parquet_file_size: int = None

    @classmethod
    def setup_class_post_hook(cls):
        context.attach_boto_session(cls.boto_ses)
        s3dir_bucket = S3Path.from_s3_uri(f"s3://{cls.bucket}/")
        cls.s3_loc = S3Location(
            s3uri_staging=s3dir_bucket.joinpath("staging").to_dir().uri,
            s3uri_datalake=s3dir_bucket.joinpath("datalake").to_dir().uri,
        )
        s3dir_snapshot = s3dir_bucket.joinpath("snapshot").to_dir()
        cls.n_db_snapshot_file = 100
        cls.n_db_snapshot_record = 1000
        epoch = datetime(1970, 1, 1)
        start_time = datetime(2021, 1, 1)
        end_time = datetime(2021, 12, 31, 23, 59, 59)
        start_ts = int((start_time - epoch).total_seconds())
        end_ts = int((end_time - epoch).total_seconds())
        df = pl.DataFrame(
            {
                "order_id": range(1, 1 + cls.n_db_snapshot_record),
                "order_time": np.random.randint(
                    start_ts, end_ts + 1, cls.n_db_snapshot_record
                ),
                "amount": np.random.rand(cls.n_db_snapshot_record) * 1000,
            }
        )
        df = df.with_columns(
            pl.concat_str([pl.lit("order-"), pl.col("order_id").cast(pl.Utf8)]).alias(
                "order_id"
            ),
            pl.from_epoch(pl.col("order_time")).alias("order_time"),
        )
        cls.n_record_per_file = cls.n_db_snapshot_record // cls.n_db_snapshot_file
        data_file_list = list()
        for ith, sub_df in enumerate(
            df.iter_slices(n_rows=cls.n_record_per_file),
            start=1,
        ):
            s3path = s3dir_snapshot.joinpath(f"{ith}.parquet")
            size, n_record, etag = write_parquet_to_s3(
                df=sub_df,
                s3path=s3path,
                s3_client=cls.s3_client,
            )
            data_file = {
                KeyEnum.URI: s3path.uri,
                KeyEnum.ETAG: etag,
                KeyEnum.SIZE: size,
                KeyEnum.N_RECORD: n_record,
            }
            data_file_list.append(data_file)
        cls.db_snapshot_manifest_file = DBSnapshotManifestFile.new(
            uri=cls.s3_loc.s3dir_staging_file_group_manifest.joinpath(
                "manifest-data.parquet"
            ).uri,
            uri_summary=cls.s3_loc.s3dir_staging_file_group_manifest.joinpath(
                "manifest-summary.json"
            ).uri,
            data_file_list=data_file_list,
            details={"create_by": "test_end_to_end"},
            calculate=True,
        )
        cls.db_snapshot_manifest_file.write(s3_client=cls.s3_client)
        cls.target_db_snapshot_file_group_size = (
            int(cls.db_snapshot_manifest_file.size // 10) + 1
        )
        cls.target_parquet_file_size = 128_000_000

    def batch_read_snapshot_data_file(
        self,
        db_snapshot_file_group_manifest_file: DBSnapshotFileGroupManifestFile,
    ):
        s3path_list = [
            S3Path.from_s3_uri(data_file[KeyEnum.URI])
            for data_file in db_snapshot_file_group_manifest_file.data_file_list
        ]
        df = read_many_parquet_from_s3(
            s3path_list=s3path_list,
            s3_client=self.s3_client,
        )
        return df

    @logger.start_and_end(
        msg="{func_name}",
    )
    def step_1_1_plan_snapshot_to_staging(self):
        step_1_1_plan_snapshot_to_staging(
            s3_client=self.s3_client,
            s3_loc=self.s3_loc,
            db_snapshot_manifest_file=self.db_snapshot_manifest_file,
            target_size=self.target_db_snapshot_file_group_size,
            logger=logger,
        )

    @logger.start_and_end(
        msg="{func_name}",
    )
    def step_1_2_process_db_snapshot_file_group_manifest_file(self):
        db_snapshot_file_group_manifest_file_list = (
            step_1_2_get_snapshot_to_staging_todo_list(
                s3_client=self.s3_client,
                s3_loc=self.s3_loc,
            )
        )
        extract_record_id = DerivedColumn(
            extractor="order_id",
            alias="record_id",
        )
        extract_create_time = DerivedColumn(
            extractor=pl.col("order_time"),
            alias="create_time",
        )
        extract_update_time = DerivedColumn(
            extractor=pl.col("create_time"),
            alias="update_time",
        )
        extract_partition_keys = [
            DerivedColumn(
                extractor=pl.col("create_time").dt.year(),
                alias="year",
            ),
            DerivedColumn(
                extractor=pl.col("create_time").dt.month().cast(pl.Utf8).str.zfill(2),
                alias="month",
            ),
        ]
        new_step_1_3_process_db_snapshot_file_group_manifest_file = (
            logger.start_and_end(
                msg="{func_name}",
            )(step_1_3_process_db_snapshot_file_group_manifest_file)
        )
        for (
            db_snapshot_file_group_manifest_file
        ) in db_snapshot_file_group_manifest_file_list:
            with logger.nested():
                new_step_1_3_process_db_snapshot_file_group_manifest_file(
                    s3_client=self.s3_client,
                    s3_loc=self.s3_loc,
                    db_snapshot_file_group_manifest_file=db_snapshot_file_group_manifest_file,
                    batch_read_func=self.batch_read_snapshot_data_file,
                    extract_record_id=extract_record_id,
                    extract_create_time=extract_create_time,
                    extract_update_time=extract_update_time,
                    extract_partition_keys=extract_partition_keys,
                    logger=logger,
                )
            break

    @logger.start_and_end(
        msg="{func_name}",
    )
    def step_2_1_plan_staging_to_datalake(self):
        step_2_1_plan_staging_to_datalake(
            s3_client=self.s3_client,
            s3_loc=self.s3_loc,
            target_size=self.target_parquet_file_size,
            logger=logger,
        )

    @logger.start_and_end(
        msg="{func_name}",
    )
    def step_2_2_process_partition_file_group_manifest_file(self):
        partition_file_group_manifest_file_list = (
            step_2_2_get_staging_to_datalake_todo_list(
                s3_client=self.s3_client,
                s3_loc=self.s3_loc,
            )
        )
        new_step_2_3_process_partition_file_group_manifest_file = logger.start_and_end(
            msg="{func_name}",
        )(step_2_3_process_partition_file_group_manifest_file)
        for (
            partition_file_group_manifest_file
        ) in partition_file_group_manifest_file_list:
            with logger.nested():
                new_step_2_3_process_partition_file_group_manifest_file(
                    s3_client=self.s3_client,
                    s3_loc=self.s3_loc,
                    partition_file_group_manifest_file=partition_file_group_manifest_file,
                    update_at_col="update_at",
                    logger=logger,
                )
            break

    def _test(self):
        with logger.disabled(
            disable=True,  # no log
            # disable=False,  # show log
        ):
            self.step_1_1_plan_snapshot_to_staging()
        with logger.disabled(
            disable=True,  # no log
            # disable=False,  # show log
        ):
            self.step_1_2_process_db_snapshot_file_group_manifest_file()
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.step_2_1_plan_staging_to_datalake()
        with logger.disabled(
            disable=True,  # no log
            # disable=False,  # show log
        ):
            self.step_2_2_process_partition_file_group_manifest_file()

    def test(self):
        with logger.disabled(
            # disable=True, # no log
            disable=False,  # show log
        ):
            logger.info("")
            self._test()


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake", preview=False)
