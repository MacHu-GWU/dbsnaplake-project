# -*- coding: utf-8 -*-

import random

from s3pathlib import S3Path

from dbsnaplake.manifest import (
    SnapshotDataFile,
    SnapshotManifestFile,
)
from dbsnaplake.vendor.timer import DateTimeTimer
from dbsnaplake.tests.mock_aws import BaseMockAwsTest


class Test(BaseMockAwsTest):
    def test(self):
        n_file = 1000
        uri = f"s3://{self.bucket}/manifest.json"
        s3path = S3Path(uri)
        snapshot_manifest_file = SnapshotManifestFile(
            uri=uri,
            snapshot_data_file_list=[
                SnapshotDataFile(
                    uri=f"s3://{self.bucket}/data/{ith}.parquet",
                    size=random.randint(1000 * 1000, 10 * 1000 * 1000),
                )
                for ith in range(1, 1 + n_file)
            ],
        )

        # display = True
        display = False
        with DateTimeTimer("Write v1", display=display):
            snapshot_manifest_file.write_v1(s3_client=self.s3_client)
        with DateTimeTimer("Write v2", display=display):
            snapshot_manifest_file.write_v2(s3_client=self.s3_client)
        with DateTimeTimer("Read v1", display=display):
            snapshot_manifest_file = SnapshotManifestFile.read_v1(
                uri=uri,
                s3_client=self.s3_client,
            )
        with DateTimeTimer("Read v2", display=display):
            snapshot_manifest_file = SnapshotManifestFile.read_v2(
                uri=uri,
                s3_client=self.s3_client,
            )

        with DateTimeTimer("Group Files", display=display):
            tasks = snapshot_manifest_file.group_files_into_tasks(
                target_size=100 * 1000 * 1000
            )


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake.manifest", preview=False)
