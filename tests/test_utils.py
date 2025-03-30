"""Tests for utility modules."""

import unittest
import os
import tempfile
import shutil
from pathlib import Path
import sys

# Add the project root to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.file_service import FileService

class TestFileService(unittest.TestCase):
    """Test the FileService class."""
    
    def setUp(self):
        """Set up test environment."""
        self.file_service = FileService()
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, 'test_file.txt')
        
        # Create test file
        with open(self.test_file, 'w') as f:
            f.write('test content')
    
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.test_dir)
    
    def test_get_file_info(self):
        """Test the get_file_info method."""
        info = self.file_service.get_file_info(self.test_file)
        
        self.assertEqual(info['name'], 'test_file.txt')
        self.assertEqual(info['extension'], 'txt')
        self.assertEqual(info['size'], 12)  # 'test content' is 12 bytes
        self.assertFalse(info['is_dir'])
    
    def test_ensure_directory(self):
        """Test the ensure_directory method."""
        test_subdir = os.path.join(self.test_dir, 'subdir', 'nested')
        
        # Directory should not exist yet
        self.assertFalse(os.path.exists(test_subdir))
        
        # Create directory
        result = self.file_service.ensure_directory(test_subdir)
        
        # Check result and directory existence
        self.assertTrue(result)
        self.assertTrue(os.path.exists(test_subdir))
        self.assertTrue(os.path.isdir(test_subdir))
    
    def test_move_file(self):
        """Test the move_file method."""
        dest_file = os.path.join(self.test_dir, 'moved_file.txt')
        
        # File should not exist at destination yet
        self.assertFalse(os.path.exists(dest_file))
        
        # Move file
        result = self.file_service.move_file(self.test_file, dest_file)
        
        # Check result and file existence
        self.assertTrue(result)
        self.assertFalse(os.path.exists(self.test_file))
        self.assertTrue(os.path.exists(dest_file))
        
        # Check content
        with open(dest_file, 'r') as f:
            content = f.read()
        self.assertEqual(content, 'test content')

if __name__ == '__main__':
    unittest.main() 