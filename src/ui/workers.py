from PyQt6.QtCore import QObject, pyqtSignal, QRunnable, QThreadPool
from typing import List, Dict, Any, Optional, Callable
import os
import time
import traceback

from src.core.aws_client import AWSClient, AWSError
from src.utils import get_logger, get_config, get_file_service

logger = get_logger()
config = get_config()
file_service = get_file_service()

class WorkerSignals(QObject):
    """Signals for worker threads."""
    started = pyqtSignal()
    finished = pyqtSignal()
    error = pyqtSignal(str, object)  # error message and exception object
    progress = pyqtSignal(int, str)  # progress value and status message
    data = pyqtSignal(object)  # generic data signal
    success = pyqtSignal(str)  # success message

class WorkerManager(QObject):
    """Manages background workers and limits concurrent operations."""
    
    def __init__(self, max_concurrent=None):
        super().__init__()
        self.thread_pool = QThreadPool.globalInstance()
        self.max_concurrent = max_concurrent or config.get('max_concurrent_operations', 5)
        self.active_workers = {}  # id -> worker mapping
        
        # Set thread pool limit
        self.thread_pool.setMaxThreadCount(self.max_concurrent)
        logger.info(f"Worker manager initialized with {self.max_concurrent} max concurrent operations")
    
    def start_worker(self, worker, worker_id=None):
        """Start a worker in the thread pool."""
        if worker_id is None:
            worker_id = str(time.time())
            
        # Store the worker reference
        self.active_workers[worker_id] = worker
        
        # Connect cleanup signal
        worker.signals.finished.connect(lambda: self._cleanup_worker(worker_id))
        worker.signals.error.connect(lambda msg, exc: self._cleanup_worker(worker_id))
        
        # Start the worker
        logger.info(f"Starting worker {worker_id}")
        worker.signals.started.emit()
        self.thread_pool.start(worker)
        
        return worker_id
    
    def cancel_worker(self, worker_id):
        """Cancel a worker by ID."""
        if worker_id in self.active_workers:
            logger.info(f"Cancelling worker {worker_id}")
            worker = self.active_workers[worker_id]
            worker.cancel()
            return True
        return False
    
    def cancel_all_workers(self):
        """Cancel all active workers."""
        logger.info(f"Cancelling all workers ({len(self.active_workers)} active)")
        for worker_id, worker in list(self.active_workers.items()):
            worker.cancel()
        return len(self.active_workers)
    
    def get_active_workers(self):
        """Get the number of active workers."""
        return len(self.active_workers)
    
    def _cleanup_worker(self, worker_id):
        """Remove a worker from the active workers list."""
        if worker_id in self.active_workers:
            logger.info(f"Cleaning up worker {worker_id}")
            del self.active_workers[worker_id]

class BaseWorker(QRunnable):
    """Base class for all workers with cancellation support."""
    def __init__(self, aws_client: AWSClient):
        super().__init__()
        self.aws_client = aws_client
        self.signals = WorkerSignals()
        self._is_cancelled = False
        self.operation_id = str(time.time())
    
    def run(self):
        """This method should be overridden by subclasses."""
        raise NotImplementedError("Subclasses must implement run()")
    
    def cancel(self):
        """Cancel the operation."""
        logger.info(f"Worker {self.operation_id} cancelled")
        self._is_cancelled = True
    
    def is_cancelled(self) -> bool:
        """Check if the operation is cancelled."""
        return self._is_cancelled
    
    def _handle_exception(self, e: Exception, operation: str):
        """Handle exceptions in worker threads."""
        logger.error(f"Error in {operation}: {str(e)}")
        error_message = str(e)
        
        # Get detailed error information if available
        if isinstance(e, AWSError):
            details = f"Operation: {e.operation}, Code: {e.code}" if e.code else f"Operation: {e.operation}"
            error_message = f"{e.message} - {details}"
        else:
            # Log the full traceback for unexpected errors
            logger.error(traceback.format_exc())
        
        self.signals.error.emit(error_message, e)

class ListBucketsWorker(BaseWorker):
    """Worker for listing S3 buckets."""
    def __init__(self, aws_client: AWSClient):
        super().__init__(aws_client)

    def run(self):
        """List all buckets."""
        try:
            if self.is_cancelled():
                return
                
            self.signals.progress.emit(0, "Listing buckets...")
            
            buckets = self.aws_client.list_buckets()
            
            if self.is_cancelled():
                return
                
            self.signals.data.emit(buckets)
            self.signals.progress.emit(100, "Buckets listed successfully")
            self.signals.success.emit(f"Found {len(buckets)} buckets")
            self.signals.finished.emit()
            
        except Exception as e:
            self._handle_exception(e, "list_buckets")

class ListObjectsWorker(BaseWorker):
    """Worker for listing objects in a bucket with improved pagination."""
    def __init__(self, aws_client: AWSClient, bucket: str, prefix: str = "", recursive: bool = True):
        super().__init__(aws_client)
        self.bucket = bucket
        self.prefix = prefix
        self.recursive = recursive

    def run(self):
        """List objects in a bucket."""
        try:
            if self.is_cancelled():
                return
            
            status_msg = f"Listing objects in {self.bucket}" + (f" with prefix: {self.prefix}" if self.prefix else "...")
            self.signals.progress.emit(0, status_msg)
            
            result = self.aws_client.list_objects(self.bucket, self.prefix, recursive=self.recursive)
            
            if self.is_cancelled():
                return
            
            # Extract relevant information
            objects = result.get('objects', [])
            prefixes = result.get('prefixes', [])
            
            self.signals.data.emit(result)
            self.signals.progress.emit(100, f"Listed {len(objects)} objects and {len(prefixes)} directories")
            self.signals.success.emit(f"Found {len(objects)} objects and {len(prefixes)} directories")
            self.signals.finished.emit()
            
        except Exception as e:
            self._handle_exception(e, "list_objects")

class UploadWorker(BaseWorker):
    """Worker for uploading files to S3 with progress tracking."""
    def __init__(self, aws_client: AWSClient, file_path: str, bucket: str, key: str, extra_args: Dict[str, Any] = None):
        super().__init__(aws_client)
        self.file_path = file_path
        self.bucket = bucket
        self.key = key
        self.extra_args = extra_args or {}
        self.status = f"Uploading {os.path.basename(file_path)} to {bucket}/{key}"

    def run(self):
        """Upload a file to S3."""
        try:
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(0, self.status)
            
            # Get file info for logging
            file_info = file_service.get_file_info(self.file_path)
            file_size = file_info.get('size', 0)
            
            # Use the AWS client's upload_file method with progress callback
            def progress_callback(progress):
                if self.is_cancelled():
                    return False
                self.signals.progress.emit(progress, self.status)
                return True
            
            # Upload the file
            self.aws_client.upload_file(
                self.file_path, 
                self.bucket, 
                self.key,
                extra_args=self.extra_args,
                progress_callback=progress_callback
            )
            
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(100, self.status)
            self.signals.success.emit(f"Uploaded {file_size} bytes to {self.bucket}/{self.key}")
            self.signals.finished.emit()
            
        except Exception as e:
            self._handle_exception(e, "upload_file")

class DownloadWorker(BaseWorker):
    """Worker for downloading files from S3 with progress tracking."""
    def __init__(self, aws_client: AWSClient, bucket: str, key: str, save_path: str):
        super().__init__(aws_client)
        self.bucket = bucket
        self.key = key
        self.save_path = save_path
        self.status = f"Downloading {key} from {bucket}"

    def run(self):
        """Download a file from S3."""
        try:
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(0, self.status)
            
            # Use the AWS client's download_file method with progress callback
            def progress_callback(progress):
                if self.is_cancelled():
                    return False
                self.signals.progress.emit(progress, self.status)
                return True
            
            # Download the file
            success = self.aws_client.download_file(
                self.bucket, 
                self.key, 
                self.save_path,
                progress_callback=progress_callback
            )
            
            if self.is_cancelled() or not success:
                return
            
            # Get the file size for reporting
            file_info = file_service.get_file_info(self.save_path)
            file_size = file_info.get('size', 0)
            
            self.signals.progress.emit(100, self.status)
            self.signals.success.emit(f"Downloaded {file_size} bytes to {os.path.basename(self.save_path)}")
            self.signals.finished.emit()
            
        except Exception as e:
            self._handle_exception(e, "download_file")

class DeleteWorker(BaseWorker):
    """Worker for deleting objects from S3."""
    def __init__(self, aws_client: AWSClient, bucket: str, key: str):
        super().__init__(aws_client)
        self.bucket = bucket
        self.key = key
        self.status = f"Deleting {key} from {bucket}"

    def run(self):
        """Delete an object from S3."""
        try:
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(0, self.status)
            
            # Delete the object
            self.aws_client.delete_object(self.bucket, self.key)
            
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(100, self.status)
            self.signals.success.emit(f"Deleted {self.key} from {self.bucket}")
            self.signals.finished.emit()
            
        except Exception as e:
            self._handle_exception(e, "delete_object")

class UploadDirectoryWorker(BaseWorker):
    """Worker for uploading directories recursively to S3."""
    def __init__(self, aws_client: AWSClient, directory_path: str, bucket: str, prefix: str):
        super().__init__(aws_client)
        self.directory_path = directory_path
        self.bucket = bucket
        self.prefix = prefix
        self.dir_name = os.path.basename(directory_path)
        self.status = f"Uploading directory {self.dir_name} to {bucket}/{prefix}"
        self.total_files = 0
        self.uploaded_files = 0
        self.total_size = 0
        self.uploaded_size = 0

    def run(self):
        """Upload a directory recursively to S3."""
        try:
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(0, f"Analyzing directory {self.dir_name}...")
            
            # First, scan the directory to get total size and file count
            self._scan_directory()
            
            if self.total_files == 0:
                self.signals.progress.emit(100, f"No files found in {self.dir_name}")
                self.signals.success.emit(f"No files found in {self.dir_name}")
                self.signals.finished.emit()
                return
            
            if self.is_cancelled():
                return
            
            # Create a directory marker in S3 (empty object with trailing slash)
            dir_key = f"{self.prefix.rstrip('/')}/{self.dir_name}/"
            
            # Start uploading files
            self.signals.progress.emit(0, f"Uploading {self.total_files} files ({self._format_size(self.total_size)})...")
            
            # Use recursive function to upload files
            success = self._upload_directory_recursive(self.directory_path, dir_key)
            
            if self.is_cancelled() or not success:
                return
            
            self.signals.progress.emit(100, self.status)
            self.signals.success.emit(f"Uploaded {self.total_files} files ({self._format_size(self.total_size)}) to {self.bucket}/{dir_key}")
            self.signals.finished.emit()
            
        except Exception as e:
            self._handle_exception(e, "upload_directory")
    
    def _scan_directory(self):
        """Scan the directory to get total size and file count."""
        try:
            for root, _, files in os.walk(self.directory_path):
                for file in files:
                    if self.is_cancelled():
                        return
                    
                    file_path = os.path.join(root, file)
                    file_info = file_service.get_file_info(file_path)
                    
                    if file_info and not file_info.get('is_dir', False):
                        self.total_files += 1
                        self.total_size += file_info.get('size', 0)
                        
            logger.info(f"Directory scan: {self.total_files} files, {self.total_size} bytes")
            
        except Exception as e:
            logger.error(f"Error scanning directory: {str(e)}")
            raise
    
    def _upload_directory_recursive(self, current_dir, s3_prefix):
        """Recursively upload a directory to S3."""
        try:
            for item in os.listdir(current_dir):
                if self.is_cancelled():
                    return False
                    
                item_path = os.path.join(current_dir, item)
                item_info = file_service.get_file_info(item_path)
                
                # Skip if we can't get info
                if not item_info:
                    continue
                
                # Handle directories
                if item_info.get('is_dir', False):
                    # Create directory marker
                    dir_key = f"{s3_prefix}{item}/"
                    
                    # Recursively upload subdirectory
                    success = self._upload_directory_recursive(item_path, dir_key)
                    if not success:
                        return False
                
                # Handle files
                else:
                    file_size = item_info.get('size', 0)
                    file_key = f"{s3_prefix}{item}"
                    
                    self.signals.progress.emit(
                        int(self.uploaded_size * 100 / max(1, self.total_size)),
                        f"Uploading {item} ({self._format_size(file_size)})..."
                    )
                    
                    # Upload the file
                    def progress_callback(progress):
                        if self.is_cancelled():
                            return False
                            
                        # Calculate the actual bytes uploaded for this file
                        file_uploaded = file_size * progress / 100
                        
                        # Calculate overall progress based on previously uploaded files plus this file
                        total_uploaded = self.uploaded_size + file_uploaded
                        total_progress = int(total_uploaded * 100 / max(1, self.total_size))
                        
                        self.signals.progress.emit(
                            total_progress,
                            f"Uploading {item} ({progress}%)..."
                        )
                        return True
                    
                    # Upload the file
                    try:
                        self.aws_client.upload_file(
                            item_path,
                            self.bucket,
                            file_key,
                            progress_callback=progress_callback
                        )
                        
                        # Update progress tracking
                        self.uploaded_files += 1
                        self.uploaded_size += file_size
                        
                    except Exception as e:
                        logger.error(f"Error uploading {item_path}: {str(e)}")
                        self.signals.error.emit(f"Error uploading {item}: {str(e)}", e)
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error in recursive upload: {str(e)}")
            self.signals.error.emit(f"Error uploading directory: {str(e)}", e)
            return False
    
    def _format_size(self, size_bytes):
        """Format bytes to human-readable size."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"

class DownloadDirectoryWorker(BaseWorker):
    """Worker for downloading directories recursively from S3."""
    def __init__(self, aws_client: AWSClient, bucket: str, prefix: str, save_path: str):
        super().__init__(aws_client)
        self.bucket = bucket
        self.prefix = prefix.rstrip('/') + '/'
        self.save_path = save_path
        self.dir_name = os.path.basename(prefix.rstrip('/'))
        self.status = f"Downloading directory {self.dir_name} from {bucket}"
        self.total_objects = 0
        self.downloaded_objects = 0
        self.total_size = 0
        self.downloaded_size = 0

    def run(self):
        """Download a directory recursively from S3."""
        try:
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(0, f"Listing objects in {self.prefix}...")
            
            # First, list all objects with the prefix to get total size and count
            result = self.aws_client.list_objects(self.bucket, self.prefix, delimiter='')
            objects = result.get('objects', [])
            
            if not objects:
                self.signals.progress.emit(100, f"No objects found in {self.prefix}")
                self.signals.success.emit(f"No objects found in {self.prefix}")
                self.signals.finished.emit()
                return
            
            # Calculate total size and count
            for obj in objects:
                if obj.get('is_directory', False):
                    continue
                self.total_objects += 1
                self.total_size += obj.get('size', 0)
            
            if self.is_cancelled():
                return
            
            # Create the target directory
            target_dir = os.path.join(self.save_path, self.dir_name)
            file_service.ensure_directory(target_dir)
            
            # Start downloading files
            self.signals.progress.emit(0, f"Downloading {self.total_objects} files ({self._format_size(self.total_size)})...")
            
            # Download each object
            for obj in objects:
                if self.is_cancelled():
                    return
                
                key = obj.get('key', '')
                size = obj.get('size', 0)
                
                # Skip directory markers
                if obj.get('is_directory', False) or key.endswith('/'):
                    continue
                
                # Calculate relative path from prefix
                if key.startswith(self.prefix):
                    rel_path = key[len(self.prefix):]
                else:
                    rel_path = key
                
                # Construct local path
                local_path = os.path.join(target_dir, rel_path)
                
                # Ensure directory exists
                file_service.ensure_directory(os.path.dirname(local_path))
                
                self.signals.progress.emit(
                    int(self.downloaded_size * 100 / max(1, self.total_size)),
                    f"Downloading {os.path.basename(key)} ({self._format_size(size)})..."
                )
                
                # Download the file
                def progress_callback(progress):
                    if self.is_cancelled():
                        return False
                    
                    # Calculate the actual bytes downloaded for this file
                    file_downloaded = size * progress / 100
                    
                    # Calculate overall progress based on previously downloaded files plus this file
                    total_downloaded = self.downloaded_size + file_downloaded
                    total_progress = int(total_downloaded * 100 / max(1, self.total_size))
                    
                    self.signals.progress.emit(
                        total_progress,
                        f"Downloading {os.path.basename(key)} ({progress}%)..."
                    )
                    return True
                
                # Download the file
                try:
                    self.aws_client.download_file(
                        self.bucket,
                        key,
                        local_path,
                        progress_callback=progress_callback
                    )
                    
                    # Update progress tracking
                    self.downloaded_objects += 1
                    self.downloaded_size += size
                    
                except Exception as e:
                    logger.error(f"Error downloading {key}: {str(e)}")
                    self.signals.error.emit(f"Error downloading {os.path.basename(key)}: {str(e)}", e)
                    return
            
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(100, self.status)
            self.signals.success.emit(f"Downloaded {self.downloaded_objects} files ({self._format_size(self.downloaded_size)}) to {target_dir}")
            self.signals.finished.emit()
            
        except Exception as e:
            self._handle_exception(e, "download_directory")
    
    def _format_size(self, size_bytes):
        """Format bytes to human-readable size."""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"

class DeleteDirectoryWorker(BaseWorker):
    """Worker for deleting directories recursively from S3."""
    def __init__(self, aws_client: AWSClient, bucket: str, prefix: str):
        super().__init__(aws_client)
        self.bucket = bucket
        self.prefix = prefix.rstrip('/') + '/'
        self.status = f"Deleting directory {self.prefix} from {bucket}"
        self.total_objects = 0
        self.deleted_objects = 0

    def run(self):
        """Delete a directory recursively from S3."""
        try:
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(0, f"Listing objects in {self.prefix}...")
            
            # First, list all objects with the prefix
            result = self.aws_client.list_objects(self.bucket, self.prefix, delimiter='')
            objects = result.get('objects', [])
            
            if not objects:
                self.signals.progress.emit(100, f"No objects found in {self.prefix}")
                self.signals.success.emit(f"No objects found in {self.prefix}")
                self.signals.finished.emit()
                return
            
            self.total_objects = len(objects)
            
            if self.is_cancelled():
                return
            
            # Start deleting objects
            self.signals.progress.emit(0, f"Deleting {self.total_objects} objects...")
            
            # Delete objects in batches of up to 1000 (S3 limit)
            batch_size = 1000
            for i in range(0, len(objects), batch_size):
                if self.is_cancelled():
                    return
                
                batch = objects[i:i+batch_size]
                delete_keys = {'Objects': [{'Key': obj['key']} for obj in batch]}
                
                try:
                    self.aws_client._execute_with_retry(
                        self.aws_client.s3_client.delete_objects,
                        Bucket=self.bucket,
                        Delete=delete_keys,
                        operation_name="delete_objects"
                    )
                    
                    # Update progress
                    self.deleted_objects += len(batch)
                    progress = int(self.deleted_objects * 100 / self.total_objects)
                    self.signals.progress.emit(progress, f"Deleted {self.deleted_objects}/{self.total_objects} objects...")
                    
                except Exception as e:
                    logger.error(f"Error deleting objects batch: {str(e)}")
                    self.signals.error.emit(f"Error deleting objects: {str(e)}", e)
                    return
            
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(100, self.status)
            self.signals.success.emit(f"Deleted {self.deleted_objects} objects from {self.bucket}/{self.prefix}")
            self.signals.finished.emit()
            
        except Exception as e:
            self._handle_exception(e, "delete_directory")

class GetObjectUrlWorker(BaseWorker):
    """Worker for generating a pre-signed URL for an S3 object."""
    def __init__(self, aws_client: AWSClient, bucket: str, key: str, expiration: int = 3600):
        super().__init__(aws_client)
        self.bucket = bucket
        self.key = key
        self.expiration = expiration
        self.status = f"Generating URL for {key}"

    def run(self):
        """Generate a pre-signed URL for an S3 object."""
        try:
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(0, self.status)
            
            # Generate the URL
            url = self.aws_client.get_object_url(self.bucket, self.key, self.expiration)
            
            if self.is_cancelled():
                return
            
            # Return the URL
            self.signals.data.emit(url)
            self.signals.progress.emit(100, self.status)
            self.signals.success.emit(f"Generated URL valid for {self.expiration} seconds")
            self.signals.finished.emit()
            
        except Exception as e:
            self._handle_exception(e, "get_object_url") 