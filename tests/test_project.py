# -*- coding: utf-8 -*-

import io
import dataclasses

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


class Test(BaseMockAwsTest):
    use_mock: bool = True
    n_db_snapshot_record: int = None
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
        n_db_snapshot_file = 20
        n_db_snapshot_record = 1000
        cls.n_db_snapshot_record = n_db_snapshot_record

        # delete fake data and regenerate it
        count, size = s3dir_snapshot_data.calculate_total_size(
            bsm=cls.bsm, for_human=False
        )
        # s3dir_snapshot_manifest.delete(bsm=cls.bsm)
        # count = 0
        if count == 0:
            db_snapshot_manifest_file = generate_db_snapshot_file_data(
                s3_client=cls.s3_client,
                s3dir_snapshot=s3dir_snapshot_data,
                s3path_db_snapshot_manifest_summary=s3path_db_snapshot_manifest_summary,
                s3path_db_snapshot_manifest_data=s3path_db_snapshot_manifest_data,
                n_db_snapshot_file=n_db_snapshot_file,
                n_db_snapshot_record=n_db_snapshot_record,
            )
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
            partition_keys=["year", "month"],
            sort_by=["order_time"],
            descending=[True],
            target_parquet_file_size=target_parquet_file_size,
            polars_writer=None,
            gzip_compression=False,
            # this only works on real S3, not on mock
            # set use_mock = False and test it locally,
            # afterward, set count_column = None bevore commit to git
            # count_column="order_id",
            count_column=None,
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
        s3path_list = self.project.s3_loc.s3dir_datalake.iter_objects(
            bsm=self.s3_client
        ).all()
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

    def _test_use_partition_use_parquet(self):
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
            self.project.s3_loc.s3dir_staging.delete()
            self.project.s3_loc.s3dir_datalake.delete()
            self.project.task_model_step_1_1_plan_snapshot_to_staging.delete_all()
            self.project.partition_keys = []
            self.project.polars_writer = Writer(format="csv")
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
            self.project.s3_loc.s3dir_staging.delete()
            self.project.s3_loc.s3dir_datalake.delete()
            self.project.task_model_step_1_1_plan_snapshot_to_staging.delete_all()
            self.project.partition_keys = []
            self.project.polars_writer = Writer(format="ndjson")
            self.project.step_1_1_plan_snapshot_to_staging()
            self.project.step_1_2_process_db_snapshot_file_group_manifest_file()
            self.project.step_2_1_plan_staging_to_datalake()
            self.project.step_2_2_process_partition_file_group_manifest_file()
            self.project.step_3_1_validate_datalake()
            self.run_analysis_on_ndjson()

    def _test(self):
        self._test_use_partition_use_parquet()
        self._test_no_partition_use_csv()
        self._test_no_partition_use_ndjson()

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
