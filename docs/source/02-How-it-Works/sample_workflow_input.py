workflow_input = {
    "table_arn": "aws:dynamodb:us-east-1:123456789012:table/my_table",
    "export_time": "2024-08-14T04:00:00+00:00",
    "s3uri_staging_dir": "s3://my-staging-bucket/staging/",
    "s3uri_database_dir": "s3://my-datalake-bucket/database/mydatabase/",
    "s3uri_python_module": "s3://my-staging-bucket/settings_modulee.py",
    "sort_by": ["update_time"],
    "descending": [False],
}
