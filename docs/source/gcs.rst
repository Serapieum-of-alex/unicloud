GCS Class Documentation
=============================

The ``GCS`` class provides methods to interact with Google Cloud Storage (GCS), allowing for uploading and downloading files.

.. class:: GCS(project_name, service_key=None)

   Initializes the GCS client with the specified project name and optional service key.

   :param project_name: The Google Cloud project name.
   :type project_name: str
   :param service_key: The path to the service account key file (optional).
   :type service_key: str, optional

   .. method:: upload_data(file_path, destination)

      Uploads a file to GCS.

      :param file_path: The path to the file to upload.
      :type file_path: str
      :param destination: The destination path in GCS.
      :type destination: str

      **Example**:

      .. code-block:: python

         gcs_client = GCS('my-gcp-project')
         gcs_client.upload_data('/local/path/to/file.txt', 'bucket-name/object-name.txt')

   .. method:: download_data(source, file_path)

      Downloads a file from GCS.

      :param source: The source path in GCS.
      :type source: str
      :param file_path: The local path to save the downloaded file.
      :type file_path: str

      **Example**:

      .. code-block:: python

         gcs_client = GCS('my-gcp-project')
         gcs_client.download_data('bucket-name/object-name.txt', '/local/path/to/save/file.txt')

.. automodule:: gcs
   :members:
   :undoc-members:
   :show-inheritance:
