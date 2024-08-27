# -*- coding: utf-8 -*-

import gzip
import random

import pytest
import polars as pl
from s3pathlib import S3Path
from polars_writer.api import Writer

from dbsnaplake.polars_utils import (
    S3_METADATA_KEY_N_RECORD,
    S3_METADATA_KEY_N_COLUMN,
    configure_s3_write_options,
    configure_s3path,
    write_to_s3,
    read_parquet_from_s3,
    read_many_parquet_from_s3,
    group_by_partition,
)
from dbsnaplake.tests.mock_aws import BaseMockAwsTest


def test_configure_s3_write_options():
    df = pl.DataFrame({"id": [1, 2, 3]})

    writer = Writer(format="csv")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=False,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".csv"
    assert s3_kwargs["content_type"] == "text/plain"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_RECORD] == "3"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_COLUMN] == "1"

    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=True,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".csv.gzip"
    assert s3_kwargs["content_type"] == "text/plain"
    assert s3_kwargs["content_encoding"] == "gzip"

    writer = Writer(format="json")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=False,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".json"
    assert s3_kwargs["content_type"] == "application/json"

    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=True,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".json.gzip"
    assert s3_kwargs["content_type"] == "application/json"
    assert s3_kwargs["content_encoding"] == "gzip"

    writer = Writer(format="parquet")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=False,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".snappy.parquet"
    assert s3_kwargs["content_type"] == "application/x-parquet"
    assert s3_kwargs["content_encoding"] == "snappy"

    writer = Writer(format="parquet")
    s3_kwargs = {"metadata": {"create_by": "polars utils"}}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=True,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".gzip.parquet"
    assert s3_kwargs["metadata"]["create_by"] == "polars utils"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_RECORD] == "3"
    assert s3_kwargs["metadata"][S3_METADATA_KEY_N_COLUMN] == "1"
    assert s3_kwargs["content_type"] == "application/x-parquet"
    assert s3_kwargs["content_encoding"] == "gzip"

    writer = Writer(format="parquet", parquet_compression="snappy")
    s3_kwargs = {}
    with pytest.raises(ValueError):
        ext = configure_s3_write_options(
            df=df,
            polars_writer=writer,
            gzip_compress=True,
            s3pathlib_write_bytes_kwargs=s3_kwargs,
        )

    writer = Writer(format="parquet", parquet_compression="snappy")
    s3_kwargs = {}
    ext = configure_s3_write_options(
        df=df,
        polars_writer=writer,
        gzip_compress=False,
        s3pathlib_write_bytes_kwargs=s3_kwargs,
    )
    assert ext == ".snappy.parquet"
    assert s3_kwargs["content_type"] == "application/x-parquet"
    assert s3_kwargs["content_encoding"] == "snappy"


def test_configure_s3path():
    s3dir = S3Path("s3://bucket/key/")
    fname = "data"
    ext = ".csv"
    s3path = S3Path("s3://bucket/key/data.tsv")

    s3path_new = configure_s3path(s3dir=s3dir, fname=fname, ext=ext)
    assert s3path_new.uri == "s3://bucket/key/data.csv"

    s3path_new = configure_s3path(s3path=s3path)
    assert s3path_new.uri == "s3://bucket/key/data.tsv"

    with pytest.raises(ValueError):
        s3path_new = configure_s3path()


class Test(BaseMockAwsTest):
    use_mock: bool = True

    def test_write_to_s3(self):
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "cathy"]})
        s3path = S3Path(f"s3://{self.bucket}/1.csv")
        s3path_new, size, etag = write_to_s3(
            df=df,
            s3_client=self.s3_client,
            polars_writer=Writer(
                format="csv",
            ),
            s3path=s3path,
        )
        text = s3path_new.read_text(bsm=self.s3_client)
        assert text.splitlines() == [
            "id,name",
            "1,alice",
            "2,bob",
            "3,cathy",
        ]
        assert s3path_new.size == size
        assert s3path_new.etag == etag
        assert s3path_new.metadata == {
            "n_record": "3",
            "n_column": "2",
        }
        assert s3path_new.response["ContentType"] == "text/plain"

        s3dir = S3Path(f"s3://{self.bucket}/")
        fname = "1"
        s3path_new, size, etag = write_to_s3(
            df=df,
            s3_client=self.s3_client,
            polars_writer=Writer(
                format="ndjson",
            ),
            gzip_compress=True,
            s3dir=s3dir,
            fname=fname,
        )
        text = gzip.decompress(s3path_new.read_bytes(bsm=self.s3_client)).decode(
            "utf-8"
        )
        assert text.splitlines() == [
            '{"id":1,"name":"alice"}',
            '{"id":2,"name":"bob"}',
            '{"id":3,"name":"cathy"}',
        ]
        assert s3path_new.size == size
        assert s3path_new.etag == etag
        assert s3path_new.metadata == {
            "n_record": "3",
            "n_column": "2",
        }
        assert s3path_new.response["ContentType"] == "application/json"
        assert s3path_new.response["ContentEncoding"] == "gzip"

    def test_write_parquet_to_s3(self):
        df = pl.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "cathy"]})
        s3path = S3Path(f"s3://{self.bucket}/1.parquet")
        write_to_s3(
            df=df,
            s3_client=self.s3_client,
            polars_writer=Writer(
                format="parquet",
            ),
            s3path=s3path,
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
