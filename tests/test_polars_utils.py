# -*- coding: utf-8 -*-

import random

import polars as pl
from s3pathlib import S3Path

from dbsnaplake.polars_utils import (
    write_parquet_to_s3,
    write_data_file,
    read_parquet_from_s3,
    read_many_parquet_from_s3,
    group_by_partition,
)
from dbsnaplake.tests.mock_aws import BaseMockAwsTest


class Test(BaseMockAwsTest):
    use_mock: bool = True

    def test_write_parquet_to_s3(self):
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "cathy"]})
        s3path = S3Path(f"s3://{self.bucket}/1.parquet")
        write_parquet_to_s3(
            df=df,
            s3path=s3path,
            s3_client=self.s3_client,
        )
        df = read_parquet_from_s3(s3path=s3path, s3_client=self.s3_client)
        assert df.shape == (3, 2)
        df = read_many_parquet_from_s3(s3path_list=[s3path], s3_client=self.s3_client)
        assert df.shape == (3, 2)

    def test_group_by_partition(self):
        n_tag = 5
        tags = [f"tag-{i}" for i in range(1, 1 + n_tag)]
        n_row = 1000
        df = pl.DataFrame(
            {
                "id": range(1, 1 + n_row),
                "tag": [random.choice(tags) for _ in range(n_row)],
            }
        )
        s3dir = S3Path(f"s3://{self.bucket}/table/")
        results = group_by_partition(
            df=df,
            s3dir=s3dir,
            filename="data.parquet",
            partition_keys=["tag"],
            sort_by=["id"],
        )
        assert len(results) == n_tag
        assert sum([df.shape[0] for df, _ in results]) == n_row


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake.polars_utils", preview=False)
