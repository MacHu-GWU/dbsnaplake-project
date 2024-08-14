# -*- coding: utf-8 -*-

import typing as T
import moto
import boto3
from s3pathlib import context
from boto_session_manager import BotoSesManager

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


class BaseMockAwsTest:
    use_mock: bool = True

    mock_aws: "moto.mock_aws" = None
    bucket: str = "mybucket"
    bsm: BotoSesManager = None
    boto_ses: boto3.Session = None
    s3_client: "S3Client" = None

    @classmethod
    def setup_class(cls):
        if cls.use_mock:
            cls.mock_aws = moto.mock_aws()
            cls.mock_aws.start()

        cls.bsm = BotoSesManager(region_name="us-east-1")
        cls.boto_ses = cls.bsm.boto_ses
        context.attach_boto_session(cls.boto_ses)
        cls.s3_client = cls.boto_ses.client("s3")

        cls.s3_client.create_bucket(Bucket=cls.bucket)

        cls.setup_class_post_hook()

    @classmethod
    def setup_class_post_hook(cls):
        pass

    @classmethod
    def teardown_class(cls):
        if cls.use_mock:
            cls.mock_aws.stop()
