# -*- coding: utf-8 -*-

from s3pathlib import S3Path

from dbsnaplake.partition import (
    Partition,
    encode_hive_partition,
    get_s3dir_partition,
    get_partitions,
)
from dbsnaplake.tests.mock_aws import BaseMockAwsTest


class TestPartition:
    def test_from_uri(self):
        part = Partition.from_uri(
            s3uri="s3://bucket/data/year=01",
            s3uri_root="s3://bucket/data",
        )
        assert part.uri == "s3://bucket/data/year=01/"
        assert part.data == {"year": "01"}
        assert part.s3dir.uri == "s3://bucket/data/year=01/"

        part = Partition.from_uri(
            s3uri="s3://bucket/data/",
            s3uri_root="s3://bucket/data/",
        )
        assert part.data == {}


def test_encode_hive_partition():
    relpath = encode_hive_partition({"year": "2021", "month": "01", "day": "01"})
    assert relpath == "year=2021/month=01/day=01"

    relpath = encode_hive_partition({})
    assert relpath == ""


def test_get_s3dir_partition():
    s3dir_root = S3Path("s3://bucket/data/")
    kvs = {"year": "2021", "month": "07", "day": "01"}
    uri = get_s3dir_partition(s3dir_root, kvs).uri
    assert uri == "s3://bucket/data/year=2021/month=07/day=01/"


class Test(BaseMockAwsTest):
    use_mock: bool = True

    def test_get_partitions(self):
        # --- case 1
        s3dir_root = S3Path(f"s3://{self.bucket}/root1/")
        (s3dir_root / "year=2021/month=01/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2021/month=02/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2021/month=03/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2022/month=01/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2022/month=02/1.json").write_text("", bsm=self.s3_client)
        (s3dir_root / "year=2022/month=03/1.json").write_text("", bsm=self.s3_client)
        partitions = get_partitions(
            s3_client=self.s3_client,
            s3dir_root=s3dir_root,
        )
        assert len(partitions) == 6


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake.partition", preview=False)
