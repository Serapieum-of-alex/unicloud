import base64
import json
import os
import unittest

from unicloud.google_cloud.secrets_manager import decode, encode


class TestEncodeDecode(unittest.TestCase):
    def setUp(self):
        """Setup a mock service account file and content for testing."""
        self.mock_file_name = "mock_service_account.json"
        self.mock_content = {"type": "service_account", "project_id": "your_project_id"}
        self.json_content = json.dumps(self.mock_content)
        with open(self.mock_file_name, "w") as mock_file:
            json.dump(self.mock_content, mock_file)
        self.encoded_content = base64.b64encode(json.dumps(self.mock_content).encode())

    def tearDown(self):
        """Clean up mock file after tests run."""
        os.remove(self.mock_file_name)

    def test_encode_with_path(self):
        # Test encoding with a file path
        encoded = encode(self.mock_file_name)
        self.assertEqual(encoded, self.encoded_content)

    def test_encode_with_dictionary_content(self):
        # Test encoding with dictionary content
        encoded = encode(self.mock_content)
        self.assertEqual(encoded, self.encoded_content)

    def test_encode_with_json_content(self):
        # Test encoding with string content
        encoded = encode(self.json_content)
        self.assertEqual(encoded, self.encoded_content)

    def test_decode(self):
        # Test decoding
        decoded = decode(self.encoded_content)
        self.assertEqual(decoded, self.mock_content)

    def test_end_to_end_with_direct_content(self):
        """Original content to encode and then decode"""
        original_content = {
            "type": "service_account",
            "project_id": "your_project_id",
            "private_key_id": "your_private_key_id",
        }
        original_content_json = json.dumps(
            original_content
        )  # Convert dict to JSON string

        # Encode the original content
        encoded_json = encode(original_content_json)
        encoded_dict = encode(original_content)
        assert encoded_json == encoded_dict
        # Decode the encoded content
        decoded_content = decode(encoded_json)

        # Assert that the decoded content matches the original content
        self.assertEqual(decoded_content, original_content)
