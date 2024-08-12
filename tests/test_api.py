# -*- coding: utf-8 -*-

from dbsnaplake import api


def test():
    _ = api
    _ = api.T_RECORD
    _ = api.T_DF_SCHEMA
    _ = api.T_EXTRACTOR
    _ = api.T_OPTIONAL_KWARGS
    _ = api.repr_data_size
    _ = api.Partition
    _ = api.extract_partition_data
    _ = api.encode_hive_partition
    _ = api.get_s3dir_partition
    _ = api.get_partitions
    _ = api.write_parquet_to_s3
    _ = api.group_by_partition
    _ = api.calculate_merge_plan
    _ = api.get_merged_schema
    _ = api.harmonize_schemas
    _ = api.DataFile
    _ = api.T_DATA_FILE
    _ = api.ManifestFile
    _ = api.T_MANIFEST_FILE
    _ = api.DerivedColumn
    _ = api.SnapshotDataFile
    _ = api.StagingDataFile
    _ = api.process_db_snapshot_file_group_manifest_file


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake.api", preview=False)
