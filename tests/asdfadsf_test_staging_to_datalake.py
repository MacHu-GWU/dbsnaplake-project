# -*- coding: utf-8 -*-

import random
import uuid
from s3pathlib import S3Path, context
from s3manifesto.api import KeyEnum, ManifestFile

from dbsnaplake.constants import (
    DBSNAPLAKE_MANIFEST_FOLDER,
    MANIFEST_SUMMARY_FOLDER,
    MANIFEST_DATA_FOLDER,
)

from dbsnaplake.staging_to_datalake import (
    group_files_into_tasks,
)
from dbsnaplake.tests.mock_aws import BaseMockAwsTest


class Test(BaseMockAwsTest):
    use_mock: bool = True

    def test_group_files_into_tasks(self):
        s3dir = S3Path.from_s3_uri(f"s3://{self.bucket}/data/")
        n_manifest = 10
        n_files_per_manifest = 100
        for ith_manifest in range(1, 1 + n_manifest):
            data_file_list = list()
            for ith_file in range(1, 1 + n_files_per_manifest):
                size = random.randint(1_000_000, 20_000_000)
                n_record = int(size / 10_000)
                year = random.randint(2024)
                data_file = {
                    KeyEnum.URI: (s3dir / f"{uuid.uuid4()}.parquet").uri,
                    KeyEnum.MD5: "NA",
                    KeyEnum.SIZE: size,
                    KeyEnum.N_RECORD: n_record,
                }
                data_file_list.append(data_file)
            s3path_manifest = (
                s3dir
                / DBSNAPLAKE_MANIFEST_FOLDER
                / MANIFEST_DATA_FOLDER
                / f"manifest-data-{ith_manifest}.parquet"
            )
            s3path_manifest_summary = (
                s3dir
                / DBSNAPLAKE_MANIFEST_FOLDER
                / MANIFEST_SUMMARY_FOLDER
                / f"manifest-summary-{ith_manifest}.parquet"
            )
            manifest_file = ManifestFile.new(
                uri=s3path_manifest.uri,
                uri_summary=s3path_manifest_summary.uri,
                data_file_list=data_file_list,
                calculate=True,
            )
            manifest_file.write(s3_client=self.s3_client)

        group_files_into_tasks(
            s3_client=self.s3_client,
            s3dir_staging=s3dir,
        )

if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake.staging_to_datalake", preview=False)
