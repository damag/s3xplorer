import unittest
from unittest.mock import patch, MagicMock
from src.core.aws_client import AWSClient

class TestAWSClient(unittest.TestCase):
    def setUp(self):
        self.client = AWSClient()
    
    @patch('boto3.Session')
    def test_authenticate_with_access_key(self, mock_session):
        # Mock successful authentication
        mock_session.return_value.client.return_value = MagicMock()
        
        success = self.client.authenticate_with_access_key(
            "test_access_key",
            "test_secret_key",
            "us-east-1"
        )
        
        self.assertTrue(success)
        mock_session.assert_called_once_with(
            aws_access_key_id="test_access_key",
            aws_secret_access_key="test_secret_key",
            region_name="us-east-1"
        )
    
    @patch('boto3.Session')
    def test_authenticate_with_profile(self, mock_session):
        # Mock successful authentication
        mock_session.return_value.client.return_value = MagicMock()
        
        success = self.client.authenticate_with_profile("test_profile")
        
        self.assertTrue(success)
        self.assertEqual(self.client.current_profile, "test_profile")
        mock_session.assert_called_once_with(profile_name="test_profile")
    
    @patch('boto3.Session')
    def test_list_buckets(self, mock_session):
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_s3.list_buckets.return_value = {
            'Buckets': [
                {'Name': 'bucket1', 'CreationDate': '2023-01-01'},
                {'Name': 'bucket2', 'CreationDate': '2023-01-02'}
            ]
        }
        mock_session.return_value.client.return_value = mock_s3
        
        # Authenticate first
        self.client.authenticate_with_access_key("test_key", "test_secret")
        
        # Test listing buckets
        buckets = self.client.list_buckets()
        
        self.assertEqual(len(buckets), 2)
        self.assertEqual(buckets[0]['Name'], 'bucket1')
        self.assertEqual(buckets[1]['Name'], 'bucket2')
    
    @patch('boto3.Session')
    def test_list_objects(self, mock_session):
        # Mock S3 client
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'file1.txt', 'Size': 100, 'LastModified': '2023-01-01'},
                    {'Key': 'file2.txt', 'Size': 200, 'LastModified': '2023-01-02'}
                ]
            }
        ]
        mock_session.return_value.client.return_value = mock_s3
        
        # Authenticate first
        self.client.authenticate_with_access_key("test_key", "test_secret")
        
        # Test listing objects
        objects = self.client.list_objects("test-bucket")
        
        self.assertEqual(len(objects), 2)
        self.assertEqual(objects[0]['Key'], 'file1.txt')
        self.assertEqual(objects[1]['Key'], 'file2.txt')

if __name__ == '__main__':
    unittest.main() 