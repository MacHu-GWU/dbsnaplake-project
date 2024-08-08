# -*- coding: utf-8 -*-

import random

from s3pathlib import S3Path
from rich import print as rprint

from dbsnaplake.manifest import (
    write_ndjson_v1,
    write_ndjson_v2,
    read_ndjson_v1,
    read_ndjson_v2,
    DataFile,
    ManifestFile,
)
from dbsnaplake.vendor.timer import DateTimeTimer
from dbsnaplake.tests.mock_aws import BaseMockAwsTest


def test_read_write_performance():
    """
    Conclusion::

        n_records = 1_000_000

        {
            'Write v1': 3.487125,
            'Write v2': 1.944694,
            'Read v1': 1.504748,
            'Read v2': 0.390745
        }

        n_records = 10_000_000
        {
            'Write v1': 37.561264,
            'Write v2': 19.951817,
            'Read v1': 15.761758,
            'Read v2': 5.038208
        }
    """
    n_records = 1_000
    # n_records = 1_000_000
    prefix = "s3://mybucket/data"
    records = [
        {
            "uri": f"{prefix}/{ith}.parquet",
            "size": random.randint(1000 * 1000, 10 * 1000 * 1000),
        }
        for ith in range(1, 1 + n_records)
    ]

    # display = True
    display = False

    result = {}

    with DateTimeTimer("Write v1", display=display) as timer:
        b = write_ndjson_v1(records)
    result["Write v1"] = timer.elapsed
    size1 = len(b)

    with DateTimeTimer("Write v2", display=display) as timer:
        b = write_ndjson_v2(records)
    result["Write v2"] = timer.elapsed
    size2 = len(b)

    with DateTimeTimer("Read v1", display=display) as timer:
        records1 = read_ndjson_v1(b)
    result["Read v1"] = timer.elapsed
    assert len(records1) == n_records
    if n_records <= 1000:
        assert records1 == records

    with DateTimeTimer("Read v2", display=display) as timer:
        records2 = read_ndjson_v2(b)
    result["Read v2"] = timer.elapsed
    assert len(records2) == n_records
    if n_records <= 1000:
        assert records2 == records

    if display:
        rprint(result)


class TestManifestFile(BaseMockAwsTest):
    def test(self):
        # make dummy manifest file
        n_file = 1000
        uri = f"s3://{self.bucket}/manifest.json"
        s3path = S3Path(uri)
        data_file_list = list()
        for ith in range(1, 1 + n_file):
            uri = f"s3://{self.bucket}/data/{ith}.parquet"
            n_record = random.randint(1000, 10 * 1000)
            size = n_record * 1000
            data_file = DataFile(
                uri=uri,
                size=size,
                n_record=n_record,
            )
            data_file_list.append(data_file)

        # test write and read
        manifest_file = ManifestFile(uri=uri, data_file_list=data_file_list)
        assert manifest_file.size is None
        manifest_file.write(s3_client=self.s3_client)
        assert isinstance(manifest_file.size, int)

        manifest_file1 = ManifestFile.read(uri=uri, s3_client=self.s3_client)
        assert manifest_file1.size == manifest_file.size
        assert manifest_file1.n_record == manifest_file.n_record
        assert len(manifest_file1.data_file_list) == len(manifest_file.data_file_list)

        # test group files into tasks
        target_size = 100_000_000  # 100MB
        data_file_group_list = manifest_file.group_files_into_tasks(
            target_size=target_size,
        )
        for data_file_group in data_file_group_list:
            assert (
                sum([data_file.size for data_file in data_file_group])
                <= target_size * 2
            )


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake.manifest", preview=False)
