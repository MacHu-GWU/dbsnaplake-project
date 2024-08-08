# -*- coding: utf-8 -*-

from parquet_dynamodb.vendor.db_snapshot_to_s3_datalake import api


def test():
    _ = api
    _ = api.T_RECORD
    _ = api.T_DF_SCHEMA
    _ = api.T_EXTRACTOR
    _ = api.T_OPTIONAL_KWARGS
    _ = api.Partition
    _ = api.get_partitions
    _ = api.encode_hive_partition
    _ = api.S3Location
    _ = api.write_parquet_to_s3
    _ = api.group_by_partition
    _ = api.DerivedColumn
    _ = api.File
    _ = api.FileGroup
    _ = api.calculate_merge_plan
    _ = api.get_merged_schema
    _ = api.harmonize_schemas
    _ = api.SnapshotDataFile
    _ = api.T_SNAPSHOT_DATA_FILE
    _ = api.StagingDataFile
    _ = api.process_snapshot_data_file
    _ = api.DatalakeDataFile
    _ = api.execute_compaction


if __name__ == "__main__":
    from parquet_dynamodb.tests import run_cov_test

    run_cov_test(
        __file__,
        "parquet_dynamodb.vendor.db_snapshot_to_s3_datalake.api",
        preview=False,
    )
