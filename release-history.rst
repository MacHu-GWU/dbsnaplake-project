.. _release_history:

Release and Version History
==============================================================================


x.y.z (Backlog)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

**Minor Improvements**

**Bugfixes**

**Miscellaneous**


0.3.2 (2024-08-31)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Bugfixes**

- Fix a bug that when writing the dataframe to final datalake, it should write to the root S3 folder, not the partition folder.
- Fix a bug that when dealing without partition, it fail to print the right s3 uri logging.
- Fix a bug that when validating the datalake in deltalake format, it uses the wrong storage options from the writer.


0.3.1 (2024-08-28)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**💥Breaking Changes**

- Removed the following public APIs. We no longer uses parameter to custom the ``batch_read_snapshot_data_file_func`` logic, all the data transformation logic should be implemented in the ``batch_read_snapshot_data_file_func`` function.
    - ``dbsnaplake.api.T_EXTRACTOR``
    - ``dbsnaplake.api.DerivedColumn``
- Removed the following writer. We start using `polars_writer <https://github.com/MacHu-GWU/polars_writer-project>`_ to write parquet files.
    - ``dbsnaplake.api.write_parquet_to_s3``
    - ``dbsnaplake.api.write_data_file``
- Add ``polars_writer`` parameter to the following API:
    - ``dbsnaplake.api.step_2_3_process_partition_file_group_manifest_file``
    - ``dbsnaplake.api.Project``

**Features and Improvements**

- No longer force to use parquet as the datalake format. Now you can use any format that supported by ``polars``.
- Add support for deltalake format.
- Allow to skip creating the datalake. This is useful when the user only wants to export the data but not to create the datalake.
- Add the following public APIs:
    - ``dbsnaplake.api.constants.S3_METADATA_KEY_N_RECORD``


0.2.1 (2024-08-16)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Minor Improvements**

- Now the ``column`` parameter is optional in ``dbsnaplake.api.validate_datalake``.
- Add the following public APIs:
    - ``dbsnaplake.api.S3Location.s3path_validate_datalake_result``
    - ``dbsnaplake.api.step_3_1_validate_datalake``
    - ``dbsnaplake.api.Project.step_3_1_validate_datalake``


0.1.2 (2024-08-16)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Features and Improvements**

- Add the following public APIs that forgot to add:
    - ``dbsnaplake.api.ValidateDatalakeResult``
    - ``dbsnaplake.api.validate_datalake``


0.1.1 (2024-08-15)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- First release
- Add the following public APIs:

    - ``dbsnaplake.api.constants``
    - ``dbsnaplake.api.constants.COL_RECORD_ID``
    - ``dbsnaplake.api.constants.COL_CREATE_TIME``
    - ``dbsnaplake.api.constants.COL_UPDATE_TIME``
    - ``dbsnaplake.api.constants.S3_METADATA_KEY_SIZE``
    - ``dbsnaplake.api.constants.S3_METADATA_KEY_N_RECORD``
    - ``dbsnaplake.api.constants.S3_METADATA_KEY_SNAPSHOT_DATA_FILE``
    - ``dbsnaplake.api.constants.S3_METADATA_KEY_STAGING_PARTITION``
    - ``dbsnaplake.api.constants.MANIFESTS_FOLDER``
    - ``dbsnaplake.api.constants.DATALAKE_FOLDER``
    - ``dbsnaplake.api.constants.SNAPSHOT_FILE_GROUPS_FOLDER``
    - ``dbsnaplake.api.constants.STAGING_FILE_GROUPS_FOLDER``
    - ``dbsnaplake.api.constants.PARTITION_FILE_GROUPS_FOLDER``
    - ``dbsnaplake.api.constants.MANIFEST_SUMMARY_FOLDER``
    - ``dbsnaplake.api.constants.MANIFEST_DATA_FOLDER``
    - ``dbsnaplake.api.T_RECORD``
    - ``dbsnaplake.api.T_DF_SCHEMA``
    - ``dbsnaplake.api.T_EXTRACTOR``
    - ``dbsnaplake.api.T_OPTIONAL_KWARGS``
    - ``dbsnaplake.api.repr_data_size``
    - ``dbsnaplake.api.S3Location``
    - ``dbsnaplake.api.Partition``
    - ``dbsnaplake.api.extract_partition_data``
    - ``dbsnaplake.api.encode_hive_partition``
    - ``dbsnaplake.api.get_s3dir_partition``
    - ``dbsnaplake.api.get_partitions``
    - ``dbsnaplake.api.write_parquet_to_s3``
    - ``dbsnaplake.api.write_data_file``
    - ``dbsnaplake.api.read_parquet_from_s3``
    - ``dbsnaplake.api.read_many_parquet_from_s3``
    - ``dbsnaplake.api.group_by_partition``
    - ``dbsnaplake.api.get_merged_schema``
    - ``dbsnaplake.api.harmonize_schemas``
    - ``dbsnaplake.api.dummy_logger``
    - ``dbsnaplake.api.DBSnapshotManifestFile``
    - ``dbsnaplake.api.DBSnapshotManifestFile.split_into_groups``
    - ``dbsnaplake.api.DBSnapshotFileGroupManifestFile``
    - ``dbsnaplake.api.DBSnapshotFileGroupManifestFile.read_all_groups``
    - ``dbsnaplake.api.DerivedColumn``
    - ``dbsnaplake.api.StagingFileGroupManifestFile``
    - ``dbsnaplake.api.T_BatchReadSnapshotDataFileCallable``
    - ``dbsnaplake.api.process_db_snapshot_file_group_manifest_file``
    - ``dbsnaplake.api.extract_s3_directory``
    - ``dbsnaplake.api.PartitionFileGroupManifestFile``
    - ``dbsnaplake.api.PartitionFileGroupManifestFile.plan_partition_compaction``
    - ``dbsnaplake.api.PartitionFileGroupManifestFile.read_all_groups``
    - ``dbsnaplake.api.process_partition_file_group_manifest_file``
    - ``dbsnaplake.api.T_TASK``
    - ``dbsnaplake.api.create_orm_model``
    - ``dbsnaplake.api.step_1_1_plan_snapshot_to_staging``
    - ``dbsnaplake.api.step_1_2_get_snapshot_to_staging_todo_list``
    - ``dbsnaplake.api.step_1_3_process_db_snapshot_file_group_manifest_file``
    - ``dbsnaplake.api.step_2_1_plan_staging_to_datalake``
    - ``dbsnaplake.api.step_2_2_get_staging_to_datalake_todo_list``
    - ``dbsnaplake.api.step_2_3_process_partition_file_group_manifest_file``
    - ``dbsnaplake.api.logger``
    - ``dbsnaplake.api.Project``
    - ``dbsnaplake.api.Project.step_1_1_plan_snapshot_to_staging``
    - ``dbsnaplake.api.Project.step_1_2_process_db_snapshot_file_group_manifest_file``
    - ``dbsnaplake.api.Project.step_2_1_plan_staging_to_datalake``
    - ``dbsnaplake.api.Project.step_2_2_process_partition_file_group_manifest_file``
