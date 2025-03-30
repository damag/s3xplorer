from PyQt6.QtWidgets import (QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QTreeView, QTableView, QPushButton, QLabel,
                             QStatusBar, QMenuBar, QMenu, QToolBar, QFileDialog,
                             QMessageBox, QGroupBox, QSplitter, QListWidget, QListWidgetItem,
                             QTableWidget, QTableWidgetItem, QHeaderView)
from PyQt6.QtCore import Qt, QSize, QTimer, QSettings
from PyQt6.QtGui import QAction, QIcon
from src.ui.auth_dialog import AuthDialog
from src.ui.models import BucketTreeModel, ObjectTableModel, S3ObjectTreeModel
from src.core.aws_client import AWSClient
from src.ui.workers import (WorkerSignals, ListBucketsWorker, ListObjectsWorker,
                           UploadWorker, DownloadWorker, DeleteWorker, WorkerManager)
from src.ui.operations_window import OperationsWindow
import os

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.aws_client = None
        self.current_bucket = None
        self.worker_manager = WorkerManager()
        self.operations_window = OperationsWindow(self)  # Create immediately
        self.verbose_mode = False
        
        # Create central widget
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        
        # Create main layout
        self.main_layout = QVBoxLayout(self.central_widget)
        self.main_layout.setContentsMargins(10, 10, 10, 10)
        self.main_layout.setSpacing(10)
        
        self.setup_ui()
        
        # Connect signals
        self.operations_window.operation_cancelled.connect(self.handle_operation_cancel)
        
        # Restore window geometry
        self.settings = QSettings("S3xplorer", "S3xplorer")
        self.restore_geometry()
        
        # Show the main window first
        self.show()
        
        # Show authentication dialog on startup
        QTimer.singleShot(100, self.handle_connect)  # Small delay to ensure window is visible
    
    def setup_ui(self):
        """Setup the main window UI."""
        self.setWindowTitle("S3 Explorer")
        self.setMinimumSize(800, 600)
        
        # Create toolbar
        toolbar = QHBoxLayout()
        toolbar.setSpacing(10)
        
        # Add connect button
        connect_button = QPushButton("Connect")
        connect_button.setIcon(QIcon.fromTheme("network-connect"))
        connect_button.clicked.connect(self.handle_connect)
        toolbar.addWidget(connect_button)
        
        # Add refresh button
        refresh_button = QPushButton("Refresh")
        refresh_button.setIcon(QIcon.fromTheme("view-refresh"))
        refresh_button.clicked.connect(self.handle_refresh)
        toolbar.addWidget(refresh_button)
        
        # Add upload button
        upload_button = QPushButton("Upload")
        upload_button.setIcon(QIcon.fromTheme("document-open"))
        upload_button.clicked.connect(self.handle_upload)
        toolbar.addWidget(upload_button)
        
        # Add download button
        download_button = QPushButton("Download")
        download_button.setIcon(QIcon.fromTheme("document-save"))
        download_button.clicked.connect(self.handle_download)
        toolbar.addWidget(download_button)
        
        # Add delete button
        delete_button = QPushButton("Delete")
        delete_button.setIcon(QIcon.fromTheme("edit-delete"))
        delete_button.clicked.connect(self.handle_delete)
        toolbar.addWidget(delete_button)
        
        toolbar.addStretch()
        
        # Add toolbar to main layout
        self.main_layout.addLayout(toolbar)
        
        # Create main content area (splitter)
        content_splitter = QSplitter(Qt.Orientation.Vertical)
        
        # Create top section with buckets, directories, and files
        top_section = QWidget()
        top_layout = QVBoxLayout(top_section)
        top_layout.setContentsMargins(0, 0, 0, 0)
        
        # Create horizontal splitter for buckets, directories, and files
        horizontal_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Buckets panel
        buckets_panel = QWidget()
        buckets_layout = QVBoxLayout(buckets_panel)
        buckets_layout.setContentsMargins(0, 0, 0, 0)
        
        # Buckets header
        buckets_header = QLabel("Buckets")
        buckets_header.setStyleSheet("font-size: 14px; font-weight: bold; padding: 5px;")
        buckets_layout.addWidget(buckets_header)
        
        # Buckets list
        self.buckets_list = QListWidget()
        self.buckets_list.setSelectionMode(QListWidget.SelectionMode.SingleSelection)
        self.buckets_list.currentItemChanged.connect(self.on_bucket_selected)
        buckets_layout.addWidget(self.buckets_list)
        
        # Directories panel
        directories_panel = QWidget()
        directories_layout = QVBoxLayout(directories_panel)
        directories_layout.setContentsMargins(0, 0, 0, 0)
        
        # Directories header
        directories_header = QLabel("Directories")
        directories_header.setStyleSheet("font-size: 14px; font-weight: bold; padding: 5px;")
        directories_layout.addWidget(directories_header)
        
        # Directories tree
        self.directories_tree = QTreeView()
        self.directories_tree.setSelectionMode(QTreeView.SelectionMode.SingleSelection)
        self.directories_tree.setHeaderHidden(True)
        self.directories_tree.setIndentation(20)
        self.directories_tree.setExpandsOnDoubleClick(True)
        self.directories_tree.clicked.connect(self.on_directory_selected)
        directories_layout.addWidget(self.directories_tree)
        
        # Files panel
        files_panel = QWidget()
        files_layout = QVBoxLayout(files_panel)
        files_layout.setContentsMargins(0, 0, 0, 0)
        
        # Files header
        files_header = QLabel("Files")
        files_header.setStyleSheet("font-size: 14px; font-weight: bold; padding: 5px;")
        files_layout.addWidget(files_header)
        
        # Files list
        self.files_list = QTableWidget()
        self.files_list.setColumnCount(3)
        self.files_list.setHorizontalHeaderLabels(["Name", "Size", "Modified"])
        self.files_list.setSelectionMode(QTableWidget.SelectionMode.ExtendedSelection)
        self.files_list.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        
        # Set column widths
        self.files_list.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.files_list.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        self.files_list.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        self.files_list.setColumnWidth(1, 100)  # Size column
        self.files_list.setColumnWidth(2, 150)  # Modified column
        
        files_layout.addWidget(self.files_list)
        
        # Add panels to horizontal splitter
        horizontal_splitter.addWidget(buckets_panel)
        horizontal_splitter.addWidget(directories_panel)
        horizontal_splitter.addWidget(files_panel)
        horizontal_splitter.setSizes([150, 250, 400])
        
        top_layout.addWidget(horizontal_splitter)
        
        # Create bottom section for operations
        bottom_section = QWidget()
        bottom_layout = QVBoxLayout(bottom_section)
        bottom_layout.setContentsMargins(0, 0, 0, 0)
        
        # Operations table
        self.operations_table = self.operations_window.operations_table
        bottom_layout.addWidget(self.operations_table)
        
        # Add sections to the vertical splitter
        content_splitter.addWidget(top_section)
        content_splitter.addWidget(bottom_section)
        content_splitter.setSizes([400, 200])  # Set initial sizes for top and bottom sections
        
        # Add content splitter to main layout
        self.main_layout.addWidget(content_splitter)
        
        # Set window style
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f5f5f5;
            }
            QPushButton {
                padding: 5px 10px;
                border: 1px solid #ccc;
                border-radius: 3px;
                background-color: white;
            }
            QPushButton:hover {
                background-color: #e0e0e0;
            }
            QPushButton:pressed {
                background-color: #d0d0d0;
            }
            QListWidget {
                border: 1px solid #ccc;
                border-radius: 3px;
                background-color: white;
            }
            QListWidget::item {
                padding: 5px;
            }
            QListWidget::item:selected {
                background-color: #e3f2fd;
                color: #1976d2;
            }
            QLabel {
                color: #333;
            }
            QProgressBar {
                border: 1px solid #ccc;
                border-radius: 3px;
                text-align: center;
            }
            QProgressBar::chunk {
                background-color: #2196f3;
            }
            QTableWidget {
                border: 1px solid #ccc;
                border-radius: 3px;
                background-color: white;
            }
            QTableWidget::item {
                padding: 5px;
            }
            QHeaderView::section {
                background-color: #f5f5f5;
                padding: 5px;
                border: 1px solid #ccc;
                font-weight: bold;
            }
            QSplitter::handle {
                background-color: #ccc;
            }
            QSplitter::handle:horizontal {
                width: 1px;
            }
            QSplitter::handle:vertical {
                height: 1px;
            }
        """)
        
        # Create status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")
    
    def debug_print(self, message: str):
        """Print debug message if verbose mode is enabled."""
        if self.verbose_mode:
            print(message)
    
    def format_size(self, size: int) -> str:
        """Format the size in bytes to a human-readable string."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"
    
    def create_toolbar(self):
        toolbar = QToolBar()
        toolbar.setMovable(False)
        toolbar.setIconSize(QSize(24, 24))
        self.addToolBar(toolbar)
        
        # Add toolbar actions
        upload_action = QAction("Upload", self)
        upload_action.setShortcut("Ctrl+U")
        upload_action.triggered.connect(self.handle_upload)
        toolbar.addAction(upload_action)
        
        download_action = QAction("Download", self)
        download_action.setShortcut("Ctrl+D")
        download_action.triggered.connect(self.handle_download)
        toolbar.addAction(download_action)
        
        toolbar.addSeparator()
        
        delete_action = QAction("Delete", self)
        delete_action.setShortcut("Delete")
        delete_action.triggered.connect(self.handle_delete)
        toolbar.addAction(delete_action)
    
    def setup_ui_state(self):
        """Set initial window state."""
        # Set column widths for the objects table
        self.objects_table.horizontalHeader().setStretchLastSection(True)
    
    def set_verbose_mode(self, verbose: bool):
        """Set verbose mode for the application."""
        self.verbose_mode = verbose
        if self.aws_client:
            self.aws_client.set_verbose_mode(verbose)
    
    def handle_connect(self):
        """Handle AWS connection."""
        dialog = AuthDialog(self)
        if dialog.exec() == AuthDialog.DialogCode.Accepted:
            self.aws_client = dialog.get_aws_client()
            self.aws_client.set_verbose_mode(self.verbose_mode)
            self.refresh_buckets()
    
    def toggle_operations_window(self):
        """Toggle the visibility of the operations window."""
        if self.operations_window.isVisible():
            self.operations_window.hide()
        else:
            self.operations_window.show()
    
    def refresh_buckets(self):
        """Refresh the list of buckets."""
        if not self.aws_client:
            return
        
        # Create signals for the worker
        signals = WorkerSignals()
        signals.data.connect(self.handle_buckets_data)
        signals.error.connect(self.handle_worker_error)
        signals.progress.connect(self.handle_worker_progress)
        
        # Create and start the worker
        operation_id = self.operations_window.add_operation("List Buckets", "Listing all buckets...")
        worker = ListBucketsWorker(self.aws_client, signals)
        self.worker_manager.start_worker(worker, operation_id)
        self.status_bar.showMessage("Refreshing buckets...")
    
    def handle_buckets_data(self, buckets):
        """Handle the buckets data from the worker."""
        self.buckets_list.clear()
        for bucket in buckets:
            self.buckets_list.addItem(bucket['Name'])
        self.status_bar.showMessage(f"Found {len(buckets)} buckets")
        
        # Complete the List Buckets operation
        for operation_id, operation in self.operations_window.operations.items():
            if operation['type'] == "List Buckets":
                self.operations_window.complete_operation(operation_id)
                break
    
    def on_bucket_selected(self, current, previous):
        """Handle bucket selection in the list widget."""
        if not current or not self.aws_client:
            return
            
        bucket_name = current.text()
        self.current_bucket = bucket_name
        
        # Create signals for the worker
        signals = WorkerSignals()
        signals.data.connect(self.handle_objects_data)
        signals.error.connect(self.handle_worker_error)
        signals.progress.connect(self.handle_worker_progress)
        
        # Create and start the worker
        operation_id = self.operations_window.add_operation(
            "List Objects", 
            f"Listing objects in bucket: {bucket_name}"
        )
        worker = ListObjectsWorker(self.aws_client, bucket_name, signals)
        self.worker_manager.start_worker(worker, operation_id)
        self.status_bar.showMessage(f"Loading objects in {bucket_name}...")
    
    def handle_objects_data(self, objects):
        """Handle the objects data from the worker."""
        # Store all objects for reference
        self.current_objects = objects
        
        # Create and set directory tree model (directories only)
        self.directory_tree_model = S3ObjectTreeModel(objects, directories_only=True)
        self.directories_tree.setModel(self.directory_tree_model)
        
        # Expand root level of directories
        self.directories_tree.expandToDepth(0)
        
        # Clear the files list
        self.files_list.setRowCount(0)
        
        # Add root files to files list
        for obj in objects:
            if "/" not in obj["Key"]:  # Files in the root directory
                self._add_file_to_table(obj)
        
        self.status_bar.showMessage(f"Found {len(objects)} objects in {self.current_bucket}")
        
        # Complete the List Objects operation
        for operation_id, operation in self.operations_window.operations.items():
            if operation['type'] == "List Objects":
                self.operations_window.complete_operation(operation_id)
                break
    
    def _add_file_to_table(self, obj):
        """Add a file to the files table."""
        row = self.files_list.rowCount()
        self.files_list.insertRow(row)
        
        # Name column
        name_item = QTableWidgetItem(os.path.basename(obj["Key"]))
        name_item.setData(Qt.ItemDataRole.UserRole, obj["Key"])
        self.files_list.setItem(row, 0, name_item)
        
        # Size column
        size_item = QTableWidgetItem(self.format_size(obj["Size"]))
        size_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.files_list.setItem(row, 1, size_item)
        
        # Modified column
        modified_item = QTableWidgetItem(obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S"))
        modified_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.files_list.setItem(row, 2, modified_item)
    
    def on_directory_selected(self, index):
        """Handle directory selection in the tree view."""
        if not index.isValid():
            return
        
        item_type = self.directory_tree_model.get_item_type(index)
        item_path = self.directory_tree_model.get_item_path(index)
        item_name = index.data()
        
        if item_type == "directory":
            # If the "Root" directory is selected, show root files
            if item_name == "/" and item_path == "":
                self.update_files_list("")
            else:
                # Update files list with files in the selected directory
                self.update_files_list(item_path)
    
    def update_files_list(self, directory_path):
        """Update files list with files in the selected directory."""
        self.files_list.setRowCount(0)
        
        # Get all objects
        objects = self.current_objects
        
        # Find all files in the selected directory
        for obj in objects:
            key = obj["Key"]
            
            # Check if file is in the selected directory
            if key.startswith(directory_path):
                # Get relative path to directory
                rel_path = key[len(directory_path):]
                
                # If there's no slash, it's a direct child
                if "/" not in rel_path and rel_path:
                    self._add_file_to_table(obj)
    
    def handle_upload(self):
        """Handle file upload."""
        if not self.aws_client or not self.current_bucket:
            QMessageBox.warning(self, "Warning", "Please select a bucket first")
            return
        
        # Get the selected directory for upload
        selected_indexes = self.directories_tree.selectedIndexes()
        prefix = ""
        
        if selected_indexes:
            index = selected_indexes[0]
            item_type = self.directory_tree_model.get_item_type(index)
            if item_type == "directory":
                prefix = self.directory_tree_model.get_item_path(index)
        
        file_path, _ = QFileDialog.getOpenFileName(self, "Select File to Upload")
        if not file_path:
            return
        
        file_name = os.path.basename(file_path)
        key = prefix + file_name
        
        # Create signals for the worker
        signals = WorkerSignals()
        signals.finished.connect(lambda: self.handle_upload_finished())
        signals.error.connect(self.handle_worker_error)
        signals.progress.connect(self.handle_worker_progress)
        
        # Create and start the worker
        operation_id = self.operations_window.add_operation(
            "Upload", 
            f"Uploading {key} to {self.current_bucket}",
            file_path,
            os.path.getsize(file_path)
        )
        worker = UploadWorker(self.aws_client, file_path, self.current_bucket, key, signals)
        self.worker_manager.start_worker(worker, operation_id)
        self.status_bar.showMessage("Uploading file...")
    
    def handle_upload_finished(self):
        """Handle successful upload."""
        self.handle_refresh()
        
        # Complete the Upload operation
        for operation_id, operation in self.operations_window.operations.items():
            if operation['type'] == "Upload":
                self.operations_window.complete_operation(operation_id)
                break
    
    def handle_download(self):
        """Handle file download."""
        if not self.aws_client or not self.current_bucket:
            QMessageBox.warning(self, "Warning", "Please select a bucket first")
            return
        
        selected_rows = self.files_list.selectedItems()
        if not selected_rows:
            QMessageBox.warning(self, "Warning", "Please select files to download")
            return
        
        save_dir = QFileDialog.getExistingDirectory(self, "Select Download Directory")
        if not save_dir:
            return
        
        # Get unique rows (since we're selecting cells)
        rows = set(item.row() for item in selected_rows)
        
        for row in rows:
            # Get the full key from the item's data
            key = self.files_list.item(row, 0).data(Qt.ItemDataRole.UserRole)
            file_name = os.path.basename(key)
            
            # Create directory structure if needed
            os.makedirs(save_dir, exist_ok=True)
            
            save_path = os.path.join(save_dir, file_name)
            
            # Create signals for the worker
            signals = WorkerSignals()
            signals.finished.connect(lambda k=key: self.handle_download_finished(k))
            signals.error.connect(lambda e, k=key: self.handle_worker_error(e, k))
            signals.progress.connect(self.handle_worker_progress)
            
            # Create and start the worker
            operation_id = self.operations_window.add_operation(
                "Download", 
                f"Downloading {key} from {self.current_bucket}",
                save_path,
                os.path.getsize(save_path) if os.path.exists(save_path) else None
            )
            worker = DownloadWorker(self.aws_client, self.current_bucket, key, save_path, signals)
            self.worker_manager.start_worker(worker, operation_id)
            self.status_bar.showMessage(f"Downloading {key}...")
    
    def handle_download_finished(self, key):
        """Handle successful download."""
        self.status_bar.showMessage(f"Downloaded {key}")
        
        # Complete the Download operation for this key
        for operation_id, operation in self.operations_window.operations.items():
            if operation['type'] == "Download" and key in operation['status']:
                self.operations_window.complete_operation(operation_id)
                break
    
    def handle_delete(self):
        """Handle object deletion."""
        if not self.aws_client or not self.current_bucket:
            QMessageBox.warning(self, "Warning", "Please select a bucket first")
            return
        
        selected_rows = self.files_list.selectedItems()
        if not selected_rows:
            QMessageBox.warning(self, "Warning", "Please select files to delete")
            return
        
        # Get unique rows (since we're selecting cells)
        rows = set(item.row() for item in selected_rows)
        
        # Confirm deletion
        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            f"Are you sure you want to delete {len(rows)} selected file(s)?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            for row in rows:
                # Get the full key from the item's data
                key = self.files_list.item(row, 0).data(Qt.ItemDataRole.UserRole)
                self._delete_object(key)
    
    def _delete_object(self, key):
        """Delete a single S3 object."""
        # Create signals for the worker
        signals = WorkerSignals()
        signals.finished.connect(lambda k=key: self.handle_delete_finished(k))
        signals.error.connect(lambda e, k=key: self.handle_worker_error(e, k))
        signals.progress.connect(self.handle_worker_progress)
        
        # Create and start the worker
        operation_id = self.operations_window.add_operation(
            "Delete", 
            f"Deleting {key} from {self.current_bucket}"
        )
        worker = DeleteWorker(self.aws_client, self.current_bucket, key, signals)
        self.worker_manager.start_worker(worker, operation_id)
        self.status_bar.showMessage(f"Deleting {key}...")
    
    def handle_delete_finished(self, key):
        """Handle successful deletion."""
        self.status_bar.showMessage(f"Deleted {key}")
        self.handle_refresh()
        
        # Complete the Delete operation for this key
        for operation_id, operation in self.operations_window.operations.items():
            if operation['type'] == "Delete" and key in operation['status']:
                self.operations_window.complete_operation(operation_id)
                break
    
    def handle_worker_progress(self, progress: int, status: str):
        """Handle progress updates from workers."""
        if self.operations_window:
            # Find the operation by matching the status text
            for operation_id, operation in self.operations_window.operations.items():
                if operation['status'] == status:
                    self.operations_window.update_progress(operation_id, progress)
                    break
    
    def handle_operation_cancel(self, operation_id: str):
        """Handle cancellation of an operation."""
        self.debug_print(f"MainWindow: Handling cancel request for operation ID: {operation_id}")
        self.worker_manager.cancel_worker(operation_id)
    
    def handle_worker_error(self, error_msg: str, context=None):
        """Handle errors from worker threads."""
        self.debug_print(f"Worker Error: {error_msg} (Context: {context})")
        
        # Find the operation ID based on context if possible (e.g., context=key for download/delete)
        # This logic might need refinement depending on how context is passed
        operation_id_to_mark = None
        if context:
            # This is a simplistic search, assumes context (like filename) is part of operation type string
            for op_id, op_data in self.operations_window.operations.items():
                if context in op_data.get('type', ''):
                    operation_id_to_mark = op_id
                    break
                    
        # Mark operation as failed in the UI (if found)
        if operation_id_to_mark:
            # Update status in the table (similar to cancel/complete)
            op_item = self.operations_window.operations_table.item(self.operations_window.operations[operation_id_to_mark]['row'], 2)
            if op_item:
                 op_item.setText("Failed")
                 op_item.setForeground(Qt.GlobalColor.red)
            # Remove cancel button
            self.operations_window.operations_table.removeCellWidget(self.operations_window.operations[operation_id_to_mark]['row'], 3)
            # Start timer to remove row
            timer = QTimer(self)
            timer.setSingleShot(True)
            # Use a lambda that captures the current operation_id_to_mark
            timer.timeout.connect(lambda op_id=operation_id_to_mark: self.operations_window.remove_operation(op_id))
            self.operations_window.completed_timers[operation_id_to_mark] = timer
            timer.start(3000) # Remove after 3 seconds
            # Remove from active operations dict
            if operation_id_to_mark in self.operations_window.operations:
                 del self.operations_window.operations[operation_id_to_mark]
        
        # Don't show message box for intentional cancellations
        if "download cancelled" in error_msg.lower():
            return
        
        QMessageBox.critical(self, "Operation Error", f"An error occurred: {error_msg}")
        self.status_bar.showMessage(f"Error: {error_msg}", 5000)
    
    def handle_refresh(self):
        """Handle refresh action."""
        if self.current_bucket:
            self.on_bucket_selected(self.buckets_list.currentItem(), None)
        else:
            self.refresh_buckets()
    
    def closeEvent(self, event):
        """Handle window close event - save geometry."""
        self.save_geometry()
        event.accept()
    
    def save_geometry(self):
        """Save window geometry to settings."""
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("windowState", self.saveState())
    
    def restore_geometry(self):
        """Restore window geometry from settings."""
        if self.settings.contains("geometry"):
            self.restoreGeometry(self.settings.value("geometry"))
        if self.settings.contains("windowState"):
            self.restoreState(self.settings.value("windowState"))
