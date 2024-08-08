# -*- coding: utf-8 -*-

import random

import polars as pl
from s3pathlib import S3Path

from dbsnaplake.compaction import (
    calculate_merge_plan,
    get_merged_schema,
    harmonize_schemas,
)


def test_calculate_merge_plan():
    files = list()
    i = 0
    for _ in range(1, 90):
        i += 1
        files.append((f"f-{i}", random.randint(1, 5)))
    for _ in range(10):
        i += 1
        files.append((f"f-{i}", random.randint(50, 100)))
    file_groups = calculate_merge_plan(files, target=64)
    for file_group in file_groups:
        size_list = [file[1] for file in file_group]
        total_size = sum(size_list)
        # print(total_size, size_list)

    files = list()
    i = 0
    for _ in range(1, 100):
        i += 1
        files.append((f"f-{i}", random.randint(32, 128)))
    file_groups = calculate_merge_plan(files, target=64)
    for file_group in file_groups:
        size_list = [file[1] for file in file_group]
        total_size = sum(size_list)
        # print(total_size, size_list)


def test_harmonize_schemas():
    df1 = pl.DataFrame(
        {
            "a": [1, 2, 3],
            "b": ["a", "b", "c"],
            "c": {
                "id": 1,
                "name": "Alice",
            },
        },
    )
    df2 = pl.DataFrame(
        {
            "a": [1, 2, 3],
        },
    )
    dfs = [df1, df2]

    schema = get_merged_schema(dfs)
    df1, df2 = harmonize_schemas([df1, df2], schema)
    # print(df1, df2)


# @moto.mock_aws
# def test_compact_parquet_files():
#     bsm = BotoSesManager(region_name="us-east-1")
#     bucket = "mybucket"
#     bsm.s3_client.create_bucket(Bucket=bucket)
#     s3dir_before = S3Path(f"s3://{bucket}/before/")
#     s3dir_after = S3Path(f"s3://{bucket}/after/")
#
#     # create test parquet files
#     ith_file = 0
#     for _ in range(1, 1 + 10):
#         ith_file += 1
#         n = random.randint(1, 2000)
#         df = pl.DataFrame(
#             {"id": list(range(1, 1 + n)), "name": ["a123456789" * 100] * n}
#         )
#         s3path = s3dir_before / f"{ith_file}.parquet"
#         buffer = io.BytesIO()
#         df.write_parquet(buffer)
#         s3path.write_bytes(buffer.getvalue())
#
#     for _ in range(1, 1 + 10):
#         ith_file += 1
#         n = random.randint(1, 2000)
#         df = pl.DataFrame(
#             {
#                 "id": list(range(1, 1 + n)),
#             }
#         )
#         s3path = s3dir_before / f"{ith_file}.parquet"
#         buffer = io.BytesIO()
#         df.write_parquet(buffer)
#         s3path.write_bytes(buffer.getvalue())


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake.compaction", preview=False)
