# -*- coding: utf-8 -*-

import dataclasses

import polars as pl
from s3pathlib import S3Path
from pynamodb_mate.api import Connection
from s3manifesto.api import KeyEnum

from dbsnaplake._import_utils import read_many_parquet_from_s3
from dbsnaplake._import_utils import DBSnapshotFileGroupManifestFile
from dbsnaplake._import_utils import DerivedColumn
from dbsnaplake._import_utils import logger
from dbsnaplake._import_utils import Project
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
        return df


class Test(BaseMockAwsTest):
    use_mock: bool = True
    n_db_snapshot_record: int = None
    project: MyProject = None

    @classmethod
    def setup_class_post_hook(cls):
        s3dir_bucket = S3Path.from_s3_uri(f"s3://{cls.bucket}/")
        s3dir_snapshot = s3dir_bucket.joinpath("snapshot").to_dir()
        s3dir_snapshot_data = s3dir_snapshot.joinpath("data").to_dir()
        s3dir_snapshot_manifest = s3dir_snapshot.joinpath("manifest").to_dir()
        s3path_db_snapshot_manifest_summary = (
            s3dir_snapshot_manifest / "manifest-summary.json"
        )
        s3path_db_snapshot_manifest_data = (
            s3dir_snapshot_manifest / "manifest-data.parquet"
        )
        n_db_snapshot_file = 100
        n_db_snapshot_record = 1000
        cls.n_db_snapshot_record = n_db_snapshot_record

        db_snapshot_manifest_file = generate_db_snapshot_file_data(
            s3_client=cls.s3_client,
            s3dir_snapshot=s3dir_snapshot_data,
            s3path_db_snapshot_manifest_summary=s3path_db_snapshot_manifest_summary,
            s3path_db_snapshot_manifest_data=s3path_db_snapshot_manifest_data,
            n_db_snapshot_file=n_db_snapshot_file,
            n_db_snapshot_record=n_db_snapshot_record,
        )

        target_db_snapshot_file_group_size = (
            int(db_snapshot_manifest_file.size // 10) + 1
        )
        target_parquet_file_size = 128_000_000

        cls.project = MyProject(
            s3_client=cls.s3_client,
            s3uri_db_snapshot_manifest_summary=s3path_db_snapshot_manifest_summary.uri,
            s3uri_staging=s3dir_bucket.joinpath("staging").to_dir().uri,
            s3uri_datalake=s3dir_bucket.joinpath("datalake").to_dir().uri,
            target_db_snapshot_file_group_size=target_db_snapshot_file_group_size,
            extract_record_id=DerivedColumn(
                extractor="order_id",
                alias="record_id",
            ),
            extract_create_time=DerivedColumn(
                extractor=pl.col("order_time"),
                alias="create_time",
            ),
            extract_update_time=DerivedColumn(
                extractor=pl.col("create_time"),
                alias="update_time",
            ),
            extract_partition_keys=[
                DerivedColumn(
                    extractor=pl.col("create_time").dt.year(),
                    alias="year",
                ),
                DerivedColumn(
                    extractor=pl.col("create_time")
                    .dt.month()
                    .cast(pl.Utf8)
                    .str.zfill(2),
                    alias="month",
                ),
            ],
            sort_by=["update_time"],
            descending=[True],
            target_parquet_file_size=target_parquet_file_size,
            tracker_table_name="dbsnaplake-tracker",
            aws_region="us-east-1",
            use_case_id="test",
        )
        cls.project.connect_dynamodb(bsm=cls.bsm)

    def run_analysis(self):
        s3path_list = self.project.s3_loc.s3dir_datalake.iter_objects(
            bsm=self.s3_client
        ).all()
        df = read_many_parquet_from_s3(
            s3path_list=s3path_list,
            s3_client=self.s3_client,
        )
        assert df.shape[0] == self.n_db_snapshot_record
        logger.info(str(df))

    def _test(self):
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_1_1_plan_snapshot_to_staging()
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
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.step_2_2_process_partition_file_group_manifest_file()
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.run_analysis()

        # no partition
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.s3_loc.s3dir_staging.delete()
            self.project.s3_loc.s3dir_datalake.delete()
            self.project.task_model_step_1_1_plan_snapshot_to_staging.delete_all()
            self.project.extract_partition_keys = None
            self.project.step_1_1_plan_snapshot_to_staging()
            self.project.step_1_2_process_db_snapshot_file_group_manifest_file()
            self.project.step_2_1_plan_staging_to_datalake()
            self.project.step_2_2_process_partition_file_group_manifest_file()
            self.run_analysis()

        # no derived column no partition
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            self.project.extract_record_id = None
            self.project.extract_create_time = None
            self.project.extract_update_time = None
            self.project.extract_partition_keys = None
            self.project.sort_by = []
            self.project.descending = []
            self.project.s3_loc.s3dir_staging.delete()
            self.project.s3_loc.s3dir_datalake.delete()
            self.project.task_model_step_1_1_plan_snapshot_to_staging.delete_all()
            self.project.step_1_1_plan_snapshot_to_staging()
            self.project.step_1_2_process_db_snapshot_file_group_manifest_file()
            self.project.step_2_1_plan_staging_to_datalake()
            self.project.step_2_2_process_partition_file_group_manifest_file()
            self.run_analysis()

    def test(self):
        with logger.disabled(
            # disable=True,  # no log
            disable=False,  # show log
        ):
            logger.info("")
            self._test()


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake", preview=False)
