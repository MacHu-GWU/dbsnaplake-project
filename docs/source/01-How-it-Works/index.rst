How it Works
==============================================================================


S3 Folder Structure
------------------------------------------------------------------------------


Staging Area
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This is the root folder of the staging area for Database snapshot to datalake ETL pipeline, it stores temporary data and materialize the intermediate state of the ETL workflow. You can use any S3 folder but I suggest using the below s3 key naming convention. Suppose that your database name is "mydatabase" and the snapshot is created at "2021-01-01T08:30:00Z" UTC time. Then you could use the following s3 folder ``s3://bucket/prefix/staging/mydatabase/snapshot=2021-01-01T08:30:00Z/`` that clearly state the database name and the snapshot time. Which will be destinguished from other snapshots created in other time.

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/


Staging Manifest
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
The ETL pipeline is primarily a divide and conquer system. Each step usually has a orchestrator to aggregate files into groups and let the distributive worker nodes to work on each file group. Each file group will have one manifest-summary file and a manifest-data file Manifest file is a highly optimized file format that stores the metadata of all files in the group. It usually

.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/
        manifest-summary.json
        manifest-data.parquet


Snapshot File Group Manifest
******************************************************************************
.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/snapshot-file-groups/
        manifest-summary/
        manifest-summary/manifest-summary-1.json
        manifest-summary/manifest-summary-2.json
        manifest-summary/...
        manifest-data/
        manifest-data/manifest-data-1.parquet
        manifest-data/manifest-data-2.parquet
        manifest-data/...


Staging File Groups Manifest
******************************************************************************
.. code-block:: python

    s3://bucket/prefix/staging/mydatabase/mytable/snapshot=2021-01-01T08:30:00Z/manifests/staging-file-groups/
        manifest-summary/
        manifest-summary/manifest-summary-1.json
        manifest-summary/manifest-summary-2.json
        manifest-summary/...
        manifest-data/
        manifest-data/manifest-data-1.parquet
        manifest-data/manifest-data-2.parquet
        manifest-data/...


Partition file Groups
******************************************************************************
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


Staging Data Lake
++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
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



Datalake Area
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
