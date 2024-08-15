# -*- coding: utf-8 -*-

"""
Usage::

    import dbsnaplake.api as dbsnaplake
"""

from . import constants
from .typehint import T_RECORD
from .typehint import T_DF_SCHEMA
from .typehint import T_EXTRACTOR
from .typehint import T_OPTIONAL_KWARGS
from .utils import repr_data_size
from .s3_loc import S3Location
from .partition import Partition
from .partition import extract_partition_data
from .partition import encode_hive_partition
from .partition import get_s3dir_partition
from .partition import get_partitions
from .polars_utils import write_parquet_to_s3
from .polars_utils import write_data_file
from .polars_utils import read_parquet_from_s3
from .polars_utils import read_many_parquet_from_s3
from .polars_utils import group_by_partition
from .compaction import get_merged_schema
from .compaction import harmonize_schemas
from .logger import dummy_logger
from .snapshot_to_staging import DBSnapshotManifestFile
from .snapshot_to_staging import DBSnapshotFileGroupManifestFile
from .snapshot_to_staging import batch_read_snapshot_data_file
from .snapshot_to_staging import DerivedColumn
from .snapshot_to_staging import StagingFileGroupManifestFile
from .snapshot_to_staging import T_BatchReadSnapshotDataFileCallable
from .snapshot_to_staging import process_db_snapshot_file_group_manifest_file
from .staging_to_datalake import extract_s3_directory
from .staging_to_datalake import PartitionFileGroupManifestFile
from .staging_to_datalake import process_partition_file_group_manifest_file
from .tracker import T_TASK
from .tracker import create_orm_model
from .project import step_1_1_plan_snapshot_to_staging
from .project import step_1_2_get_snapshot_to_staging_todo_list
from .project import step_1_3_process_db_snapshot_file_group_manifest_file
from .project import step_2_1_plan_staging_to_datalake
from .project import step_2_2_get_staging_to_datalake_todo_list
from .project import step_2_3_process_partition_file_group_manifest_file
from .project import logger
from .project import Project
