"""This module contains the secret manager for the unicloud package."""

import os.path
from typing import Union, Any
import base64
import json


def encode(secret_file: Union[str, Any]) -> bytes:
    """encode.

        encode the secret file to base64 string to be used in the environment variable.
        If the path is True, the secret_file is a path to the service account file.

    Parameters
    ----------
    secret_file: [str]
        the secret_file can be the path to the service account file, a JSON string representing the content of the
        service account file, or a dictionary representing the service account content.

    Returns
    -------
    byte string

    Examples
    --------
    To encode a secret file content:

        >>> secret_file = {"type": "service_account", "project_id": "your_project_id"}
        >>> encode(secret_file, path=False)
        b'eyJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsICJwcm9qZWN0X2lkIjogInlvdXJfcHJvamVjdF9pZCJ9'

    To encode a secret file using its path:

        >>> encode("examples/data/secret-file.json", path=True) # doctest: +SKIP
        b'eyJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsICJwcm9qZWN0X2lkIjogImV4YW1wbGUtcHJvamVjdC1pZCIs******'

    To encode a secret file using a json string representing the content:

        >>> secret_file = '{"type": "service_account", "project_id": "your_project_id"}'
        >>> encode(secret_file) # doctest: +SKIP
        b'eyJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsICJwcm9qZWN0X2lkIjogImV4YW1wbGUtcHJvamVjdF9pZCJ9'

    """
    if isinstance(secret_file, str) and os.path.exists(secret_file):
        content: dict = json.load(open(secret_file))
    elif isinstance(secret_file, dict):
        # Direct dictionary input
        content: dict = secret_file
    else:
        # a JSON string representing the content
        content: dict = json.loads(secret_file)

    # serialize first
    dumped_service_account = json.dumps(content)
    encoded_service_account = base64.b64encode(dumped_service_account.encode())
    return encoded_service_account


def decode(string: bytes) -> str:
    """decode.

        decode the base64 string to the original secret file content

    Parameters
    ----------
    string: [bytes]
        the content of the secret file encoded with base64

    Returns
    -------
    str:
        google cloud service account content

    Examples
    --------
    To decode a base64 string:

        >>> encoded_content = b'eyJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsICJwcm9qZWN0X2lkIjogImV4YW1wbGUtcHJvamVjdF9pZCJ9'
        >>> decode(encoded_content)
        {"type": "service_account", "project_id": "your_project_id"}
    """
    service_key = json.loads(base64.b64decode(string).decode())
    return service_key
