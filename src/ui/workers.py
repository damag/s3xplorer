from PyQt6.QtCore import QObject, pyqtSignal, QRunnable, QThreadPool
from typing import List, Dict, Any, Optional
from src.core.aws_client import AWSClient
import os

class WorkerSignals(QObject):
    """Signals for worker threads."""
    finished = pyqtSignal()
    error = pyqtSignal(str)
    progress = pyqtSignal(int, str)  # progress value and status message
    data = pyqtSignal(list)

class BaseWorker(QRunnable):
    """Base class for all workers with cancellation support."""
    def __init__(self, aws_client: AWSClient, signals: WorkerSignals):
        super().__init__()
        self.aws_client = aws_client
        self.signals = signals
        self._is_cancelled = False
    
    def cancel(self):
        """Cancel the operation."""
        self._is_cancelled = True
    
    def is_cancelled(self) -> bool:
        """Check if the operation is cancelled."""
        return self._is_cancelled

class ListBucketsWorker(BaseWorker):
    """Worker for listing S3 buckets."""
    def __init__(self, aws_client: AWSClient, signals: WorkerSignals):
        super().__init__(aws_client, signals)

    def run(self):
        try:
            if self.is_cancelled():
                return
            self.signals.progress.emit(0, "Listing buckets...")
            buckets = self.aws_client.list_buckets()
            if self.is_cancelled():
                return
            self.signals.data.emit(buckets)
            self.signals.progress.emit(100, "Buckets listed successfully")
            self.signals.finished.emit()
        except Exception as e:
            self.signals.error.emit(str(e))

class ListObjectsWorker(BaseWorker):
    """Worker for listing objects in a bucket."""
    def __init__(self, aws_client: AWSClient, bucket: str, signals: WorkerSignals):
        super().__init__(aws_client, signals)
        self.bucket = bucket
        self.prefix = ""  # Optional prefix for listing objects

    def run(self):
        try:
            if self.is_cancelled():
                return
            
            status_msg = f"Listing objects in {self.bucket}" + (f" with prefix: {self.prefix}" if self.prefix else "...")
            self.signals.progress.emit(0, status_msg)
            
            objects = self.aws_client.list_objects(self.bucket, self.prefix)
            
            if self.is_cancelled():
                return
            
            self.signals.data.emit(objects)
            self.signals.progress.emit(100, status_msg)
            self.signals.finished.emit()
        except Exception as e:
            self.signals.error.emit(str(e))

class UploadWorker(BaseWorker):
    """Worker for uploading files to S3."""
    def __init__(self, aws_client: AWSClient, file_path: str, bucket: str, key: str, signals: WorkerSignals):
        super().__init__(aws_client, signals)
        self.file_path = file_path
        self.bucket = bucket
        self.key = key
        self.status = f"Uploading {key} to {bucket}"

    def run(self):
        try:
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(0, self.status)
            
            # Use the AWS client's upload_file method with progress callback
            def progress_callback(progress):
                if self.is_cancelled():
                    return False
                self.signals.progress.emit(progress, self.status)
                return True
            
            success = self.aws_client.upload_file(
                self.file_path, 
                self.bucket, 
                self.key,
                progress_callback=progress_callback
            )
            
            if self.is_cancelled():
                return
            
            if success:
                self.signals.progress.emit(100, self.status)
                self.signals.finished.emit()
            else:
                self.signals.error.emit("Failed to upload file")
        except Exception as e:
            self.signals.error.emit(str(e))

class DownloadWorker(BaseWorker):
    """Worker for downloading files from S3."""
    def __init__(self, aws_client: AWSClient, bucket: str, key: str, save_path: str, signals: WorkerSignals):
        super().__init__(aws_client, signals)
        self.bucket = bucket
        self.key = key
        self.save_path = save_path
        self.status = f"Downloading {key} from {bucket}"

    def run(self):
        try:
            if self.is_cancelled():
                self.signals.error.emit("Download cancelled")
                return
            
            self.signals.progress.emit(0, self.status)
            
            # Use the AWS client's download_file method with progress callback
            def progress_callback(progress):
                if self.is_cancelled():
                    self.signals.error.emit("Download cancelled")
                    return False
                self.signals.progress.emit(progress, self.status)
                return True
            
            success = self.aws_client.download_file(
                self.bucket, 
                self.key, 
                self.save_path,
                progress_callback=progress_callback
            )
            
            if self.is_cancelled():
                self.signals.error.emit("Download cancelled")
                return
            
            if success:
                self.signals.progress.emit(100, self.status)
                self.signals.finished.emit()
            else:
                self.signals.error.emit("Failed to download file")
        except Exception as e:
            self.signals.error.emit(str(e))

class DeleteWorker(BaseWorker):
    """Worker for deleting objects from S3."""
    def __init__(self, aws_client: AWSClient, bucket: str, key: str, signals: WorkerSignals):
        super().__init__(aws_client, signals)
        self.bucket = bucket
        self.key = key

    def run(self):
        try:
            if self.is_cancelled():
                return
            
            self.signals.progress.emit(0, f"Deleting {self.key}...")
            success = self.aws_client.delete_object(self.bucket, self.key)
            
            if self.is_cancelled():
                return
            
            if success:
                self.signals.progress.emit(100, f"Deleted {self.key} successfully")
                self.signals.finished.emit()
            else:
                self.signals.error.emit("Failed to delete object")
        except Exception as e:
            self.signals.error.emit(str(e))

class WorkerManager:
    """Manages worker threads and their signals."""
    def __init__(self):
        self.thread_pool = QThreadPool()
        self.thread_pool.setMaxThreadCount(4)  # Limit concurrent threads
        self.workers: Dict[str, BaseWorker] = {}

    def start_worker(self, worker: BaseWorker, operation_id: str):
        """Start a worker thread."""
        self.workers[operation_id] = worker
        self.thread_pool.start(worker)
    
    def cancel_worker(self, operation_id: str):
        """Cancel a worker thread."""
        if operation_id in self.workers:
            self.workers[operation_id].cancel()
            del self.workers[operation_id] 