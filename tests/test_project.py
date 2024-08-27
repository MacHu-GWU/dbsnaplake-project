# -*- coding: utf-8 -*-

import os
import io
import dataclasses

import pytest
import polars as pl
from s3pathlib import S3Path
from s3manifesto.api import KeyEnum
from polars_writer.api import Writer

from dbsnaplake._import_utils import read_many_parquet_from_s3
from dbsnaplake._import_utils import DBSnapshotFileGroupManifestFile
from dbsnaplake._import_utils import logger
from dbsnaplake._import_utils import Project
from dbsnaplake._import_utils import step_1_2_get_snapshot_to_staging_todo_list
from dbsnaplake._import_utils import step_2_2_get_staging_to_datalake_todo_list
from dbsnaplake.tests.mock_aws import BaseMockAwsTest
from dbsnaplake.tests.data_faker import generate_db_snapshot_file_data


@dataclasses.dataclass
class MyProject(Project):
    def batch_read_snapshot_data_file(
        self,
        db_snapshot_file_group_manifest_file: DBSnapshotFileGroupManifestFile,
        **kwargs,
    ):
        s3path_list = [
            S3Path.from_s3_uri(data_file[KeyEnum.URI])
            for data_file in db_snapshot_file_group_manifest_file.data_file_list
        ]
        df = read_many_parquet_from_s3(
            s3path_list=s3path_list,
            s3_client=self.s3_client,
        )
        df = df.with_columns(
            year=pl.col("order_time").dt.year().cast(pl.Utf8),
            month=pl.col("order_time").dt.month().cast(pl.Utf8).str.zfill(2),
        )
        return df


class BaseTest(BaseMockAwsTest):
    """ """

    n_db_snapshot_file: int = None
    n_db_snapshot_record: int = None
    reset_db_snapshot_data: bool = True
    project: MyProject = None

    @classmethod
    def setup_class_post_hook(cls):
        s3dir_bucket = S3Path.from_s3_uri(f"s3://{cls.bucket}/projects/dbsnaplake/")
        s3dir_snapshot = s3dir_bucket.joinpath("snapshot").to_dir()
        s3dir_snapshot_data = s3dir_snapshot.joinpath("data").to_dir()
        s3dir_snapshot_manifest = s3dir_snapshot.joinpath("manifest").to_dir()
        s3path_db_snapshot_manifest_summary = (
            s3dir_snapshot_manifest / "manifest-summary.json"
        )
        s3path_db_snapshot_manifest_data = (
            s3dir_snapshot_manifest / "manifest-data.parquet"
        )
        n_db_snapshot_file = cls.n_db_snapshot_file
        n_db_snapshot_record = cls.n_db_snapshot_record

        # delete fake data and regenerate it
        def _generate_db_snapshot_file_data():
            db_snapshot_manifest_file = generate_db_snapshot_file_data(
                s3_client=cls.s3_client,
                s3dir_snapshot=s3dir_snapshot_data,
                s3path_db_snapshot_manifest_summary=s3path_db_snapshot_manifest_summary,
                s3path_db_snapshot_manifest_data=s3path_db_snapshot_manifest_data,
                n_db_snapshot_file=n_db_snapshot_file,
                n_db_snapshot_record=n_db_snapshot_record,
            )
            return db_snapshot_manifest_file

        count, size = s3dir_snapshot_data.calculate_total_size(
            bsm=cls.bsm, for_human=False
        )

        if cls.reset_db_snapshot_data:
            s3dir_snapshot_manifest.delete(bsm=cls.bsm)
            db_snapshot_manifest_file = _generate_db_snapshot_file_data()
            total_size = db_snapshot_manifest_file.size
        else:
            if count == 0:
                s3dir_snapshot_manifest.delete(bsm=cls.bsm)
                db_snapshot_manifest_file = _generate_db_snapshot_file_data()
                total_size = db_snapshot_manifest_file.size
            else:
                total_size = size

        target_db_snapshot_file_group_size = int(total_size // 10) + 1
        target_parquet_file_size = 128_000_000

        cls.project = MyProject(
            s3_client=cls.s3_client,
            s3uri_db_snapshot_manifest_summary=s3path_db_snapshot_manifest_summary.uri,
            s3uri_staging=s3dir_bucket.joinpath("staging").to_dir().uri,
            s3uri_datalake=s3dir_bucket.joinpath("datalake").to_dir().uri,
            target_db_snapshot_file_group_size=target_db_snapshot_file_group_size,
            partition_keys=[],
            create_datalake=True,
            sort_by=["order_time"],
            descending=[True],
            target_parquet_file_size=target_parquet_file_size,
            polars_writer=None,
            gzip_compression=False,
            count_column="order_id",
            tracker_table_name="dbsnaplake-tracker",
            aws_region="us-east-1",
            use_case_id="test",
        )
        cls.project.connect_dynamodb(bsm=cls.bsm)

        cls.project.s3_loc.s3dir_staging.delete(bsm=cls.bsm)
        cls.project.s3_loc.s3dir_datalake.delete(bsm=cls.bsm)
        cls.project.task_model_step_0_prepare_db_snapshot_manifest.delete_all()

    @logger.start_and_end(
        msg="{func_name}",
    )
    def run_analysis_on_parquet(self):
        s3path_list = (
            self.project.s3_loc.s3dir_datalake.iter_objects(
                bsm=self.s3_client,
            )
            .filter(lambda s3path: s3path.basename.endswith(".parquet"))
            .all()
        )
        df = read_many_parquet_from_s3(
            s3path_list=s3path_list,
            s3_client=self.s3_client,
        )
        assert df.shape[0] == self.n_db_snapshot_record
        logger.info(str(df))

    @logger.start_and_end(
        msg="{func_name}",
    )
    def run_analysis_on_csv(self):
        s3path_list = self.project.s3_loc.s3dir_datalake.iter_objects(
            bsm=self.s3_client
        ).all()
        sub_df_list = list()
        for s3path in s3path_list:
            b = s3path.read_bytes(bsm=self.s3_client)
            sub_df = pl.read_csv(io.BytesIO(b))
            sub_df_list.append(sub_df)
        df = pl.concat(sub_df_list)
        assert df.shape[0] == self.n_db_snapshot_record
        logger.info(str(df))

    @logger.start_and_end(
        msg="{func_name}",
    )
    def run_analysis_on_ndjson(self):
        s3path_list = self.project.s3_loc.s3dir_datalake.iter_objects(
            bsm=self.s3_client
        ).all()
        sub_df_list = list()
        for s3path in s3path_list:
            b = s3path.read_bytes(bsm=self.s3_client)
            sub_df = pl.read_ndjson(io.BytesIO(b))
            sub_df_list.append(sub_df)
        df = pl.concat(sub_df_list)
        assert df.shape[0] == self.n_db_snapshot_record
        logger.info(str(df))

    def reset_s3_dynamodb(self):
        self.project.s3_loc.s3dir_staging.delete()
        self.project.s3_loc.s3dir_datalake.delete()
        self.project.task_model_step_1_1_plan_snapshot_to_staging.delete_all()

    def get_storage_options_for_write(self) -> dict:
        credential = self.bsm.boto_ses.get_credentials()
        return {
            "aws_region": self.bsm.aws_region,
            "aws_access_key_id": credential.access_key,
            "aws_secret_access_key": credential.secret_key,
        }

    def get_storage_options_for_write_delta(self) -> dict:
        credential = self.bsm.boto_ses.get_credentials()
        return {
            "aws_region": self.bsm.aws_region,
            "aws_access_key_id": credential.access_key,
            "aws_secret_access_key": credential.secret_key,
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def get_storage_options_for_scan_delta(self) -> dict:
        credential = self.bsm.boto_ses.get_credentials()
        return {
            "aws_region": self.bsm.aws_region,
            "aws_access_key_id": credential.access_key,
            "aws_secret_access_key": credential.secret_key,
        }

    def _test_use_partition_use_parquet(self):
        self.reset_s3_dynamodb()
        self.project.partition_keys = ["year", "month"]
        self.project.polars_writer = Writer(format="parquet")
        if self.use_mock is False:
            self.project.polars_writer.storage_options = self.get_storage_options_for_write()
        else:
            self.project.count_column = None

        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_1_1_plan_snapshot_to_staging()
        db_snapshot_file_group_manifest_file_list = (
            step_1_2_get_snapshot_to_staging_todo_list(
                s3_client=self.s3_client,
                s3_loc=self.project.s3_loc,
            )
        )
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_1_2_process_db_snapshot_file_group_manifest_file()
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_2_1_plan_staging_to_datalake()
        partition_file_group_manifest_file_list = (
            step_2_2_get_staging_to_datalake_todo_list(
                s3_client=self.s3_client,
                s3_loc=self.project.s3_loc,
            )
        )
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_2_2_process_partition_file_group_manifest_file()
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_3_1_validate_datalake()
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.run_analysis_on_parquet()

    def _test_no_partition_use_csv(self):
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.reset_s3_dynamodb()
            self.project.partition_keys = []
            self.project.polars_writer = Writer(format="csv")
            if self.use_mock is False:
                self.project.polars_writer.storage_options = self.get_storage_options_for_write()
            else:
                self.project.count_column = None

            self.project.step_1_1_plan_snapshot_to_staging()
            self.project.step_1_2_process_db_snapshot_file_group_manifest_file()
            self.project.step_2_1_plan_staging_to_datalake()
            self.project.step_2_2_process_partition_file_group_manifest_file()
            self.project.step_3_1_validate_datalake()
            self.run_analysis_on_csv()

    def _test_no_partition_use_ndjson(self):
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.reset_s3_dynamodb()
            self.project.partition_keys = []
            self.project.polars_writer = Writer(format="ndjson")
            if self.use_mock is False:
                self.project.polars_writer.storage_options = self.get_storage_options_for_write()
            else:
                self.project.count_column = None

            self.project.step_1_1_plan_snapshot_to_staging()
            self.project.step_1_2_process_db_snapshot_file_group_manifest_file()
            self.project.step_2_1_plan_staging_to_datalake()
            self.project.step_2_2_process_partition_file_group_manifest_file()
            self.project.step_3_1_validate_datalake()
            self.run_analysis_on_ndjson()

    def _test_use_partition_use_delta(self):
        self.reset_s3_dynamodb()
        self.project.partition_keys = ["year", "month"]
        self.project.polars_writer = Writer(
            format="delta",
            delta_mode="append",
            delta_merge_options={
                "predicate": "s.order_id = t.order_id",
                "source_alias": "s",
                "target_alias": "t",
            },
        )
        self.project.count_column = "order_id"
        if self.use_mock is False:
            self.project.polars_writer.storage_options = self.get_storage_options_for_write_delta()
        else:
            self.project.count_column = None

        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_1_1_plan_snapshot_to_staging()
        db_snapshot_file_group_manifest_file_list = (
            step_1_2_get_snapshot_to_staging_todo_list(
                s3_client=self.s3_client,
                s3_loc=self.project.s3_loc,
            )
        )
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_1_2_process_db_snapshot_file_group_manifest_file()
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_2_1_plan_staging_to_datalake()
        partition_file_group_manifest_file_list = (
            step_2_2_get_staging_to_datalake_todo_list(
                s3_client=self.s3_client,
                s3_loc=self.project.s3_loc,
            )
        )
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_2_2_process_partition_file_group_manifest_file()
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_3_1_validate_datalake()
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.run_analysis_on_parquet()


class TestWithMock(BaseTest):
    use_mock = True
    n_db_snapshot_file = 20
    n_db_snapshot_record = 1000
    reset_db_snapshot_data = True

    def test(self):
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            logger.info("")
            self._test_use_partition_use_parquet()
            self._test_no_partition_use_csv()
            self._test_no_partition_use_ndjson()


@pytest.mark.skipif("CI" in os.environ, reason="Skip in CI")
class TestWithoutMock(BaseTest):
    use_mock = False
    n_db_snapshot_file = 20
    n_db_snapshot_record = 1000
    reset_db_snapshot_data = False

    def test(self):
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            logger.info("")
            self._test_use_partition_use_delta()


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake", preview=False)
