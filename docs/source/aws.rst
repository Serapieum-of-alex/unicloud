AWS Class Documentation
=============================

The ``AWS`` class provides methods to interact with Amazon Web Services (AWS) S3, facilitating the uploading and downloading of files.

.. class:: AWS(aws_access_key_id, aws_secret_access_key, region_name)

   Initializes the AWS S3 client with the necessary AWS credentials and region.

   :param aws_access_key_id: Your AWS access key ID.
   :type aws_access_key_id: str
   :param aws_secret_access_key: Your AWS secret access key.
   :type aws_secret_access_key: str
   :param region_name: The AWS region to connect to.
   :type region_name: str

   .. method:: upload(file_path, bucket_name, object_name)

      Uploads a file to AWS S3.

      :param file_path: The path to the file to upload.
      :type file_path: str
      :param bucket_name: The name of the S3 bucket.
      :type bucket_name: str
      :param object_name: The object name in S3.
      :type object_name: str

      **Example**:

      .. code-block:: python

         aws_client = AWS('your-access-key-id', 'your-secret-access-key', 'us-east-1')
         aws_client.upload_data('/local/path/to/file.txt', 'my-s3-bucket', 'path/in/bucket/file.txt')

   .. method:: download(bucket_name, object_name, file_path)

      Downloads a file from AWS S3.

      :param bucket_name: The name of the S3 bucket.
      :type bucket_name: str
      :param object_name: The object name in S3.
      :type object_name: str
      :param file_path: The local path to save the downloaded file.
      :type file_path: str

      **Example**:

      .. code-block:: python

         aws_client = AWS('your-access-key-id', 'your-secret-access-key', 'us-east-1')
         aws_client.download_data('my-s3-bucket', 'path/in/bucket/file.txt', '/local/path/to/save/file.txt')


.. automodule:: unicloud.aws
   :members:
   :undoc-members:
   :show-inheritance:
