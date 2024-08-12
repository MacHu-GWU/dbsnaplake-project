How it Works
==============================================================================


Overview
------------------------------------------------------------------------------
``dbsnaplake`` is a tool designed to convert database snapshot data into an analytics-friendly data lake structure on AWS S3. This document outlines the key steps and processes involved in this transformation.


Workflow
------------------------------------------------------------------------------
The process begins with a DB Snapshot File Manifest, which contains metadata for all database snapshot export data files. The workflow consists of two main steps: ETL (Extract, Transform, Load) to the staging area, and data compaction for optimized storage in the data lake.


Step 1: ETL to Staging Area
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DB Snapshot File Groups:

The system scans metadata of all snapshot files, collecting statistics on file count, size, and processing benchmarks.

**DB Snapshot File Groups**

Files are divided into manageable "DB Snapshot File Groups" each not exceeding X MB (X is determined by the statistics information above). A manifest file for each group is created and stored in S3.

**Staging File Groups**

- Workers process each DB Snapshot File Group, reading data into DataFrames.
- Data is partitioned based on the Partition Key and written to separate Partition Folders.
- The resulting files form "Staging File Groups" with manifests stored in S3.


Step 2: Data Compaction and Migration to Data Lake
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
**Partition File Groups**


- Small files in each partition are compacted to optimize query performance.
- The target is to create Parquet files of approximately 128MB each.
- The orchestrator scans metadata of all Staging files to package them into "Partition File Groups" with manifests stored in S3.
- Workers are assigned to process files in each "Partition File Group".


S3 Folder Structure
------------------------------------------------------------------------------


Staging Area
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This is the root folder of the staging area for Database snapshot to datalake ETL pipeline, it stores temporary data and materialize the intermediate state of the ETL workflow. You can use any S3 folder but I suggest using the below s3 key naming convention. Suppose that your database name is "mydatabase" and the snapshot is created at "2021-01-01T08:30:00Z" UTC time. Then you could use the following s3 folder ``s3://bucket/prefix/staging/mydatabase/snapshot=2021-01-01T08:30:00Z/`` that clearly state the database name and the snapshot time. Which will be destinguished from other snapshots created in other time.

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/


Staging Manifest
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Staging manifest S3 location serves as a central repository for all intermediate manifest files crucial for orchestrating the data pipeline. This folder houses metadata files that guide each step of the ETL process, from initial data ingestion to final data lake storage.

The pipeline's journey begins with a single manifest file containing metadata for the database snapshot data. As the process unfolds, additional manifest files are generated and stored in this location, each representing a specific stage or group of data being processed. These manifests act as roadmaps, enabling efficient coordination and tracking of data transformations throughout the entire workflow. We expect to see the initial manifest file at this S3 location, however, you are free to use any S3 location.

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/
        manifest-summary.json
        manifest-data.parquet


Snapshot File Group Manifest
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Snapshot File Group manifest is a crucial component of the divide-and-conquer ETL pipeline. It is created by an orchestrator that aggregates snapshot files into manageable groups. Each file group is represented by two key elements: a manifest-summary file and a manifest-data file. These manifest files are highly optimized, storing comprehensive metadata for all files within the group. This structure enables distributed worker nodes to efficiently process each file group in parallel. Snapshot File Group manifests are stored in the S3 location below.

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/snapshot-file-groups/
        manifest-summary/
        manifest-summary/manifest-summary-1.json # group 1
        manifest-summary/manifest-summary-2.json # group 2
        manifest-summary/...
        manifest-data/
        manifest-data/manifest-data-1.parquet # group 1
        manifest-data/manifest-data-2.parquet # group 2
        manifest-data/...


Staging Data Lake
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Staging datalake S3 location serves as a transitional storage area in the dbsnaplake pipeline, mirroring the structure of the final datalake but with a key difference. This location temporarily houses numerous small Parquet files generated by the ETL workers during the initial processing phase. While following the same hierarchical organization as the final datalake, the staging area contains a higher volume of smaller files. This interim state allows for efficient parallel processing but isn't optimized for query performance. Subsequently, a compaction process is applied to these files, consolidating them into larger, more query-efficient Parquet files before their final placement in the datalake. The Staging datalake S3 location is shown below.

.. code-block:: python

    # ------------------------------------------------------------------------------
    # staging datalake, this is temporary place to store the datalake data
    # we need to do some optimization before moving the data to the real datalake
    # ------------------------------------------------------------------------------
    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/datalake/
        ${database_name}/${schema_name}/${table_name}/
            ${partition_key1}=${partition_key1_value}/${partition_key2}=${partition_key2_value}/.../
                ${staging_data_file_1}
                ${staging_data_file_2}
                ...
            ${partition_key1}=${partition_key1_value}/${partition_key2}=${partition_key2_value}/.../
                ${staging_data_file_1}
                ${staging_data_file_2}
                ...
            ...


Partition file Groups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
An orchestrator scans the staging datalake, identifying small files within each partition that can be consolidated. This process aims to group these files into larger, more efficiently queryable units. The orchestrator analyzes file sizes and distribution across partitions, creating logical groups that balance optimal file size (typically around 128MB) with processing efficiency. These Partition File Groups serve as instructions for worker nodes, guiding them in the compaction process. By consolidating smaller files into larger ones, this step significantly enhances query performance and reduces overhead in the final datalake, striking a balance between storage efficiency and analytical speed. The Partition file Groups S3 location is shown below.

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/partition-file-groups/
        manifest-summary/
        manifest-summary/manifest-summary-1.json
        manifest-summary/manifest-summary-2.json
        manifest-summary/...
        manifest-data/
        manifest-data/manifest-data-1.parquet
        manifest-data/manifest-data-2.parquet
        manifest-data/...


Datalake Area
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The final datalake folder structure is shown as below.

.. code-block:: python

    s3://bucket/prefix/datalake/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/
        ${database_name}/${schema_name}/${table_name}/
                    ${partition_key1}=${partition_key1_value}/${partition_key2}=${partition_key2_value}/.../
                        ${data_file_1}
                        ${data_file_2}
                        ...
                    ${partition_key1}=${partition_key1_value}/${partition_key2}=${partition_key2_value}/.../
                        ${data_file_1}
                        ${data_file_2}
                        ...
                    ...
