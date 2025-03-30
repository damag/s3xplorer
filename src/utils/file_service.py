"""File service utilities for the application."""

import os
import shutil
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import tempfile
import hashlib
from src.utils.logging import get_logger

logger = get_logger()

class FileService:
    """Service for file operations."""
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get or create the singleton file service instance."""
        if cls._instance is None:
            cls._instance = FileService()
        return cls._instance
    
    def __init__(self):
        """Initialize the file service."""
        # Create temp directory if needed
        self.temp_dir = Path(tempfile.gettempdir()) / 's3xplorer'
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get file information."""
        path = Path(file_path)
        if not path.exists():
            logger.error(f"File not found: {file_path}")
            return {}
        
        try:
            stats = path.stat()
            return {
                'name': path.name,
                'path': str(path),
                'size': stats.st_size,
                'modified': stats.st_mtime,
                'is_dir': path.is_dir(),
                'extension': path.suffix.lower()[1:] if path.suffix else '',
            }
        except Exception as e:
            logger.error(f"Error getting file info for {file_path}: {e}")
            return {}
    
    def calculate_md5(self, file_path: str) -> Optional[str]:
        """Calculate MD5 hash of a file."""
        try:
            path = Path(file_path)
            if not path.exists() or path.is_dir():
                logger.error(f"Cannot calculate MD5 for {file_path}: not a file or doesn't exist")
                return None
            
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating MD5 for {file_path}: {e}")
            return None
    
    def ensure_directory(self, directory_path: str) -> bool:
        """Ensure a directory exists, create if it doesn't."""
        try:
            path = Path(directory_path)
            path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Error creating directory {directory_path}: {e}")
            return False
    
    def create_temp_file(self, prefix: str = "download_") -> str:
        """Create a temporary file."""
        try:
            temp_file = tempfile.NamedTemporaryFile(delete=False, prefix=prefix, dir=self.temp_dir)
            temp_file.close()
            return temp_file.name
        except Exception as e:
            logger.error(f"Error creating temporary file: {e}")
            return ""
    
    def cleanup_temp_files(self, age_hours: int = 24) -> int:
        """Remove old temporary files."""
        try:
            count = 0
            current_time = time.time()
            for file_path in self.temp_dir.glob("*"):
                if file_path.is_file():
                    file_age = current_time - file_path.stat().st_mtime
                    if file_age > age_hours * 3600:
                        file_path.unlink()
                        count += 1
            logger.info(f"Cleaned up {count} temporary files")
            return count
        except Exception as e:
            logger.error(f"Error cleaning up temporary files: {e}")
            return 0
    
    def list_files(self, directory_path: str) -> List[Dict[str, Any]]:
        """List files in a directory with file info."""
        try:
            path = Path(directory_path)
            if not path.exists() or not path.is_dir():
                logger.error(f"Cannot list files in {directory_path}: not a directory or doesn't exist")
                return []
            
            files = []
            for item in path.iterdir():
                files.append(self.get_file_info(str(item)))
            return files
        except Exception as e:
            logger.error(f"Error listing files in {directory_path}: {e}")
            return []
    
    def get_home_directory(self) -> str:
        """Get the user's home directory."""
        return str(Path.home())
    
    def get_directory_size(self, directory_path: str) -> int:
        """Calculate the total size of a directory."""
        try:
            path = Path(directory_path)
            if not path.exists() or not path.is_dir():
                logger.error(f"Cannot get size of {directory_path}: not a directory or doesn't exist")
                return 0
            
            total_size = 0
            for item in path.glob('**/*'):
                if item.is_file():
                    total_size += item.stat().st_size
            return total_size
        except Exception as e:
            logger.error(f"Error calculating directory size for {directory_path}: {e}")
            return 0
    
    def move_file(self, source: str, destination: str) -> bool:
        """Move a file from source to destination."""
        try:
            src_path = Path(source)
            dst_path = Path(destination)
            
            if not src_path.exists():
                logger.error(f"Cannot move file: source {source} does not exist")
                return False
            
            # Ensure destination directory exists
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Move the file
            shutil.move(str(src_path), str(dst_path))
            return True
        except Exception as e:
            logger.error(f"Error moving file from {source} to {destination}: {e}")
            return False


def get_file_service() -> FileService:
    """Get the file service instance."""
    return FileService.get_instance() 