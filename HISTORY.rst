=======
History
=======

0.1.0 (2024-03-01)
------------------

* google-cloud storage and amazon s3 classes.

0.2.0 (2024-10-11)
------------------

* Bump up versions of dependencies.

0.3.0 (2024-12-15)
------------------

* create a utils module.

dev
"""
* move the aws, and gcs modules to submodules `aws` and `google_cloud`.

GCS
"""
* Create a `Bucket` class that represents a Google Cloud Storage bucket.
* The `Bucket` class has methods for uploading, downloading, and deleting files.
* The `Bucket` class has methods for listing files in the bucket.

0.4.0 (2024-12-27)
------------------

dev
"""
* add logger to both the `S3` and `GCS` classes.

GCS
"""
* add `rename` method to the `Bucket` class.

AWS
"""
* the `S3` class initialization does not take the aws credentials as arguments, but read them from environment variables.
* add `get_bucket` method to the `S3` class to return a more comperhensive `Bucket` object.
* add `Bucket` class that represents an S3 bucket.
* the `Bucket` class has methods for uploading, downloading, and deleting, renaming and listing files.
