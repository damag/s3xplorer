"""Tests for configuration module."""

import unittest
import os
import tempfile
import json
import shutil
import sys
from pathlib import Path

# Add the project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import with patched home directory to avoid affecting real config
os.environ['HOME'] = tempfile.mkdtemp()
os.environ['USERPROFILE'] = os.environ['HOME']  # For Windows

from src.utils.config import Config

class TestConfig(unittest.TestCase):
    """Test the Config class."""
    
    def setUp(self):
        """Set up test environment."""
        # Create a test config directory
        self.test_dir = Path(os.environ['HOME']) / '.s3xplorer'
        self.test_dir.mkdir(parents=True, exist_ok=True)
        
        # Force a new config instance
        Config._instance = None
        self.config = Config.get_instance()
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)
    
    def test_default_config(self):
        """Test the default configuration values."""
        # Check that default values are set
        self.assertEqual(self.config.get('theme'), 'default')
        self.assertEqual(self.config.get('default_region'), 'us-east-1')
        self.assertEqual(self.config.get('max_concurrent_operations'), 5)
    
    def test_set_get_config(self):
        """Test setting and getting configuration values."""
        # Set a value
        self.config.set('test_key', 'test_value')
        
        # Get the value
        self.assertEqual(self.config.get('test_key'), 'test_value')
        
        # Check that the file was created
        config_file = self.test_dir / 'config.json'
        self.assertTrue(config_file.exists())
        
        # Check the file content
        with open(config_file, 'r') as f:
            data = json.load(f)
        self.assertEqual(data['test_key'], 'test_value')
    
    def test_nonexistent_key(self):
        """Test getting a nonexistent key."""
        # Get a nonexistent key with default
        self.assertEqual(self.config.get('nonexistent', 'default_value'), 'default_value')
        
        # Get a nonexistent key without default
        self.assertIsNone(self.config.get('nonexistent'))
    
    def test_profile_management(self):
        """Test profile management."""
        # Initial profiles should be empty
        profiles = self.config.get_profiles()
        self.assertEqual(profiles, {})
        
        # Save a profile
        profile_data = {
            'region': 'us-west-2',
            'auth_type': 'access_key',
        }
        self.config.save_profile('test_profile', profile_data)
        
        # Get profiles
        profiles = self.config.get_profiles()
        self.assertEqual(len(profiles), 1)
        self.assertEqual(profiles['test_profile'], profile_data)
        
        # Check the profiles file
        profiles_file = self.test_dir / 'profiles.json'
        self.assertTrue(profiles_file.exists())
        
        # Check the file content
        with open(profiles_file, 'r') as f:
            data = json.load(f)
        self.assertEqual(data['test_profile'], profile_data)

if __name__ == '__main__':
    unittest.main() 