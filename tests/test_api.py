# -*- coding: utf-8 -*-

from dbsnaplake import api


def test():
    _ = api
    _ = api.constants
    _ = api.constants.COL_RECORD_ID
    _ = api.constants.COL_CREATE_TIME
    _ = api.constants.COL_UPDATE_TIME
    _ = api.constants.S3_METADATA_KEY_SIZE
    _ = api.constants.S3_METADATA_KEY_N_RECORD
    _ = api.constants.S3_METADATA_KEY_SNAPSHOT_DATA_FILE
    _ = api.constants.S3_METADATA_KEY_STAGING_PARTITION
    _ = api.constants.MANIFESTS_FOLDER
    _ = api.constants.DATALAKE_FOLDER
    _ = api.constants.SNAPSHOT_FILE_GROUPS_FOLDER
    _ = api.constants.STAGING_FILE_GROUPS_FOLDER
    _ = api.constants.PARTITION_FILE_GROUPS_FOLDER
    _ = api.constants.MANIFEST_SUMMARY_FOLDER
    _ = api.constants.MANIFEST_DATA_FOLDER
    _ = api.T_RECORD
    _ = api.T_DF_SCHEMA
    _ = api.T_EXTRACTOR
    _ = api.T_OPTIONAL_KWARGS
    _ = api.repr_data_size
    _ = api.S3Location
    _ = api.Partition
    _ = api.extract_partition_data
    _ = api.encode_hive_partition
    _ = api.get_s3dir_partition
    _ = api.get_partitions
    _ = api.write_parquet_to_s3
    _ = api.write_data_file
    _ = api.read_parquet_from_s3
    _ = api.read_many_parquet_from_s3
    _ = api.group_by_partition
    _ = api.get_merged_schema
    _ = api.harmonize_schemas
    _ = api.dummy_logger
    _ = api.DBSnapshotManifestFile
    _ = api.DBSnapshotManifestFile.split_into_groups
    _ = api.DBSnapshotFileGroupManifestFile
    _ = api.DBSnapshotFileGroupManifestFile.read_all_groups
    _ = api.DerivedColumn
    _ = api.StagingFileGroupManifestFile
    _ = api.T_BatchReadSnapshotDataFileCallable
    _ = api.process_db_snapshot_file_group_manifest_file
    _ = api.extract_s3_directory
    _ = api.PartitionFileGroupManifestFile
    _ = api.PartitionFileGroupManifestFile.plan_partition_compaction
    _ = api.PartitionFileGroupManifestFile.read_all_groups
    _ = api.process_partition_file_group_manifest_file
    _ = api.T_TASK
    _ = api.create_orm_model
    _ = api.step_1_1_plan_snapshot_to_staging
    _ = api.step_1_2_get_snapshot_to_staging_todo_list
    _ = api.step_1_3_process_db_snapshot_file_group_manifest_file
    _ = api.step_2_1_plan_staging_to_datalake
    _ = api.step_2_2_get_staging_to_datalake_todo_list
    _ = api.step_2_3_process_partition_file_group_manifest_file
    _ = api.logger
    _ = api.Project
    _ = api.Project.step_1_1_plan_snapshot_to_staging
    _ = api.Project.step_1_2_process_db_snapshot_file_group_manifest_file
    _ = api.Project.step_2_1_plan_staging_to_datalake
    _ = api.Project.step_2_2_process_partition_file_group_manifest_file


if __name__ == "__main__":
    from dbsnaplake.tests import run_cov_test

    run_cov_test(__file__, "dbsnaplake.api", preview=False)
