How it Works
==============================================================================


Diagram
------------------------------------------------------------------------------
Check out the diagram below to understand the flow of the Database snapshot to datalake ETL pipeline.

.. raw:: html
    :file: ./how-it-works.drawio.html


Data Pipeline
------------------------------------------------------------------------------
The pipeline begins with a DB Snapshot File Manifest, which contains metadata for all database snapshot export data files. The workflow consists of two main steps: ETL (Extract, Transform, Load) to the staging area, and data compaction for optimized storage in the data lake.


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
.. seealso::

    :class:`~dbsnaplake.s3_loc.S3Location`


Staging Area
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This is the root folder of the staging area for Database snapshot to datalake ETL pipeline, it stores temporary data and materialize the intermediate state of the ETL workflow. You can use any S3 folder but I suggest using the below s3 key naming convention. Suppose that your database name is "mydatabase" and the snapshot is created at "2021-01-01T08:30:00Z" UTC time. Then you could use the following s3 folder ``s3://bucket/prefix/staging/mydatabase/snapshot=2021-01-01T08:30:00Z/`` that clearly state the database name and the snapshot time. Which will be destinguished from other snapshots created in other time.

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/

.. seealso::

    :meth:`~dbsnaplake.s3_loc.S3Location.s3dir_staging`


Staging Manifest
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Staging manifest S3 location serves as a central repository for all intermediate manifest files crucial for orchestrating the data pipeline. This folder houses metadata files that guide each step of the ETL process, from initial data ingestion to final data lake storage.

The pipeline's journey begins with a single manifest file containing metadata for the database snapshot data. As the process unfolds, additional manifest files are generated and stored in this location, each representing a specific stage or group of data being processed. These manifests act as roadmaps, enabling efficient coordination and tracking of data transformations throughout the entire workflow. We expect to see the initial manifest file at this S3 location, however, you are free to use any S3 location.

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/
        manifest-summary.json
        manifest-data.parquet

.. seealso::

    :meth:`~dbsnaplake.s3_loc.S3Location.s3dir_staging_manifest`


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

.. seealso::

    :meth:`~dbsnaplake.s3_loc.S3Location.s3dir_snapshot_file_group_manifest`


Staging Data Lake
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The Staging datalake S3 location serves as a transitional storage area in the dbsnaplake pipeline, mirroring the structure of the final datalake but with a key difference. This location temporarily houses numerous small Parquet files generated by the ETL workers during the initial processing phase. While following the same hierarchical organization as the final datalake, the staging area contains a higher volume of smaller files. This interim state allows for efficient parallel processing but isn't optimized for query performance. Subsequently, a compaction process is applied to these files, consolidating them into larger, more query-efficient Parquet files before their final placement in the datalake. The Staging datalake S3 location is shown below.

.. code-block:: python

    # ------------------------------------------------------------------------------
    # staging datalake, this is temporary place to store the datalake data
    # we need to do some optimization before moving the data to the real datalake
    # ------------------------------------------------------------------------------
    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/datalake/
        ${partition_key1}=${partition_key1_value}/${partition_key2}=${partition_key2_value}/.../
            ${staging_data_file_1}
            ${staging_data_file_2}
            ...
        ${partition_key1}=${partition_key1_value}/${partition_key2}=${partition_key2_value}/.../
            ${staging_data_file_1}
            ${staging_data_file_2}
            ...
        ...

.. seealso::

    :meth:`~dbsnaplake.s3_loc.S3Location.s3dir_staging_datalake`


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

.. seealso::

    :meth:`~dbsnaplake.s3_loc.S3Location.s3dir_partition_file_group_manifest`


Datalake Area
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The data lake area is the final destination for processed and optimized data. It serves as the root folder for the output data lake, where it represents a "Table".

1. Historical Snapshots:
   To maintain a history of table snapshots, use a naming convention that includes the snapshot timestamp: ``s3://bucket/prefix/datalake/mydatabase/mytable_YYYY_MM_DD_HH_MM_SS/``
2. Latest Data Only:
    To keep only the most recent data, use a static table name: ``s3://bucket/prefix/datalake/mydatabase/mytable/``

Let's assume that the data lake root folder is:

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable_2021_01_01_08_30_00/

Within each table folder, data is organized by partition keys and stored in optimized file formats (e.g., Parquet). Here's an example of the hierarchical structure:

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable_2021_01_01_08_30_00/
        ${partition_key1}=${partition_key1_value}/${partition_key2}=${partition_key2_value}/.../
            ${data_file_1}
            ${data_file_2}
            ...
        ${partition_key1}=${partition_key1_value}/${partition_key2}=${partition_key2_value}/.../
            ${data_file_1}
            ${data_file_2}
            ...
        ...

.. seealso::

    :meth:`~dbsnaplake.s3_loc.S3Location.s3dir_datalake`


S3 Location
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:class:`~dbsnaplake.s3_loc.S3Location`

.. code-block:: python

    from dbsnaplake.api import S3Location

    s3_loc = S3Location(
        s3uri_staging="s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/",
        s3uri_datalake="s3://bucket/prefix/staging/mydatabase/mytable_2021_01_01_08_30_00/"
    )

.. seealso::

    :class:`~dbsnaplake.s3_loc.S3Location`
