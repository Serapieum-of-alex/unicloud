""" S3 Cloud Storage"""
from typing import Optional
from unicloud.abstract_class import CloudStorageFactory
import boto3


class S3(CloudStorageFactory):
    """ S3 Cloud Storage"""

    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str, region_name: Optional[str] = None):
        """
        Initialize the AWS S3 client with credentials and region information.

        Parameters
        ----------
        aws_access_key_id: [str]
            Your AWS access key ID.
        aws_secret_access_key: [str]
            Your AWS secret access key.
        region_name: Optional[str]
            The name of the AWS region to connect to. Default is None.
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self._client = self.create_client()

    @property
    def client(self):
        """client."""
        return self._client

    def create_client(self):
        """
        Creates and returns an AWS S3 client instance.

        initializing the AWS S3 client, passing credentials directly is one option. Another approach is to use AWS
        IAM roles for EC2 instances or to configure the AWS CLI with aws configure, which sets up the credentials
        file used by boto3. This can be a more secure and manageable way to handle credentials, especially in
        production environments.

        Initialize the S3 client with AWS credentials and region.
        """

        return boto3.client(
            's3',
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

    def upload(self, path: str, bucket_path: str):
        """Upload a file to S3.

        Parameters
        ----------
        path: [str]
            The path to the file to upload.
        bucket_path: [str]
            The bucket_path in the format "bucket_name/object_name".
        """
        bucket_name, object_name = bucket_path.split('/', 1)
        self.client.upload_file(path, bucket_name, object_name)
        print(f"File {path} uploaded to {bucket_path}.")

    def download(self, bucket_path: str, file_path: str):
        """ Download a file from S3.

        Parameters
        ----------
        bucket_path: [str]
            The bucket_path in the format "bucket_name/object_name".
        file_path: [str]
            The path to save the downloaded file.
        """
        bucket_name, object_name = bucket_path.split('/', 1)
        self.client.download_file(bucket_name, object_name, file_path)
        print(f"File {bucket_path} downloaded to {file_path}.")
