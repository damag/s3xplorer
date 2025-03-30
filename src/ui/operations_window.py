from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                             QLabel, QProgressBar, QTableWidget, QTableWidgetItem,
                             QHeaderView, QTextBrowser, QSplitter, QDialog, QMessageBox)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QDateTime, QSize
from PyQt6.QtGui import QIcon, QColor
import uuid
import os
import time
from typing import Optional, Dict, Any

from src.utils import get_logger, get_config

logger = get_logger()
config = get_config()

class OperationDetailsDialog(QDialog):
    """Dialog to show detailed information about an operation."""
    
    def __init__(self, operation_data: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Operation Details")
        self.setMinimumSize(600, 400)
        
        # Create layout
        layout = QVBoxLayout(self)
        
        # Create details browser
        self.details_browser = QTextBrowser()
        self.details_browser.setOpenExternalLinks(True)
        layout.addWidget(self.details_browser)
        
        # Add close button
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.accept)
        close_button.setFixedWidth(100)
        
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        button_layout.addWidget(close_button)
        layout.addLayout(button_layout)
        
        # Fill with operation details
        self._populate_details(operation_data)
    
    def _populate_details(self, operation_data: Dict[str, Any]):
        """Populate the details browser with operation information."""
        html = "<html><body style='font-family: Arial; font-size: 10pt;'>"
        
        # Operation type and status
        html += f"<h2>{operation_data.get('type', 'Unknown operation')}</h2>"
        html += f"<p><b>Status:</b> {operation_data.get('status', 'Unknown')}</p>"
        
        # File information
        if operation_data.get('file_path'):
            html += f"<p><b>File:</b> {operation_data.get('file_path')}</p>"
        
        if operation_data.get('file_size') is not None:
            html += f"<p><b>Size:</b> {self._format_size(operation_data.get('file_size'))}</p>"
        
        # Progress and timing
        html += f"<p><b>Progress:</b> {operation_data.get('progress', 0)}%</p>"
        
        if operation_data.get('start_time'):
            start_time = operation_data.get('start_time').toString('yyyy-MM-dd hh:mm:ss')
            html += f"<p><b>Start time:</b> {start_time}</p>"
            
            if operation_data.get('end_time'):
                end_time = operation_data.get('end_time').toString('yyyy-MM-dd hh:mm:ss')
                html += f"<p><b>End time:</b> {end_time}</p>"
                
                # Calculate duration
                duration_ms = operation_data.get('start_time').msecsTo(operation_data.get('end_time'))
                duration_s = duration_ms / 1000.0
                
                if duration_s < 60:
                    duration_str = f"{duration_s:.1f} seconds"
                elif duration_s < 3600:
                    duration_str = f"{duration_s / 60:.1f} minutes"
                else:
                    duration_str = f"{duration_s / 3600:.1f} hours"
                
                html += f"<p><b>Duration:</b> {duration_str}</p>"
        
        # Additional details
        if operation_data.get('details'):
            html += "<h3>Additional Details</h3>"
            html += "<ul>"
            for key, value in operation_data.get('details', {}).items():
                html += f"<li><b>{key}:</b> {value}</li>"
            html += "</ul>"
        
        # Error information
        if operation_data.get('error'):
            html += "<h3>Error</h3>"
            html += f"<p style='color: red;'>{operation_data.get('error')}</p>"
        
        html += "</body></html>"
        self.details_browser.setHtml(html)
    
    def _format_size(self, size_bytes):
        """Format size in bytes to human readable string."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

class OperationsWindow(QWidget):
    """Window to display ongoing operations with improved UI and stats."""
    operation_cancelled = pyqtSignal(str)  # Signal to cancel an operation
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Operations")
        self.setMinimumWidth(800)
        self.setMinimumHeight(400)
        
        # Create main layout
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # Stats bar at the top
        stats_layout = QHBoxLayout()
        self.active_count_label = QLabel("Active: 0")
        self.completed_count_label = QLabel("Completed: 0")
        self.failed_count_label = QLabel("Failed: 0")
        
        stats_layout.addWidget(self.active_count_label)
        stats_layout.addWidget(self.completed_count_label)
        stats_layout.addWidget(self.failed_count_label)
        stats_layout.addStretch()
        
        clear_button = QPushButton("Clear Completed")
        clear_button.clicked.connect(self.clear_completed)
        stats_layout.addWidget(clear_button)
        
        layout.addLayout(stats_layout)
        
        # Create table for operations
        self.operations_table = QTableWidget()
        self.operations_table.setColumnCount(7)
        self.operations_table.setHorizontalHeaderLabels([
            "Operation", "File", "Size", "Speed", "Progress", "Status", "Actions"
        ])
        self.operations_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        self.operations_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeMode.Fixed)
        
        # Set column widths
        self.operations_table.setColumnWidth(0, 150)  # Operation
        self.operations_table.setColumnWidth(1, 200)  # File
        self.operations_table.setColumnWidth(2, 100)  # Size
        self.operations_table.setColumnWidth(3, 100)  # Speed
        self.operations_table.setColumnWidth(4, 200)  # Progress
        self.operations_table.setColumnWidth(5, 300)  # Status
        self.operations_table.setColumnWidth(6, 60)   # Actions
        
        self.operations_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.operations_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.operations_table.doubleClicked.connect(self.show_operation_details)
        
        layout.addWidget(self.operations_table)
        
        # Add button bar at the bottom
        button_layout = QHBoxLayout()
        
        cancel_all_button = QPushButton("Cancel All")
        cancel_all_button.clicked.connect(self.cancel_all_operations)
        
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.hide)
        close_button.setFixedWidth(100)
        
        button_layout.addWidget(cancel_all_button)
        button_layout.addStretch()
        button_layout.addWidget(close_button)
        
        layout.addLayout(button_layout)
        
        # Store operation data
        self.operations = {}
        self.operation_counters = {
            'active': 0,
            'completed': 0,
            'failed': 0
        }
        
        # Completion timers
        self.completed_timers = {}
        
        # Speed tracking
        self.speed_tracking = {}
        self.speed_update_timer = QTimer(self)
        self.speed_update_timer.timeout.connect(self.update_speeds)
        self.speed_update_timer.start(1000)  # Update every second
        
        # Auto-cleanup completed operations
        self.auto_cleanup_timer = QTimer(self)
        self.auto_cleanup_timer.timeout.connect(self.auto_cleanup)
        
        # Set auto-cleanup interval from config (0 = disabled)
        cleanup_interval = config.get('operations_auto_cleanup', 0)
        if cleanup_interval > 0:
            self.auto_cleanup_timer.start(cleanup_interval * 60 * 1000)  # Convert minutes to ms
    
    def format_size(self, size_bytes):
        """Format size in bytes to human readable string."""
        if size_bytes is None:
            return ""
            
        size_bytes = float(size_bytes)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"
    
    def format_speed(self, bytes_per_second):
        """Format speed in bytes per second to human readable string."""
        if bytes_per_second is None or bytes_per_second < 0:
            return ""
        return f"{self.format_size(bytes_per_second)}/s"
    
    def add_operation(self, operation_type: str, status: str, file_path: str = None, file_size: int = None):
        """Add a new operation to the window."""
        operation_id = str(uuid.uuid4())
        
        row = self.operations_table.rowCount()
        self.operations_table.insertRow(row)
        
        # Operation name
        name_item = QTableWidgetItem(operation_type)
        name_item.setFlags(name_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.operations_table.setItem(row, 0, name_item)
        
        # File name
        file_item = QTableWidgetItem(os.path.basename(file_path) if file_path else "")
        file_item.setFlags(file_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        if file_path:
            file_item.setToolTip(file_path)
        self.operations_table.setItem(row, 1, file_item)
        
        # File size
        size_item = QTableWidgetItem(self.format_size(file_size) if file_size else "")
        size_item.setFlags(size_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.operations_table.setItem(row, 2, size_item)
        
        # Speed
        speed_item = QTableWidgetItem("")
        speed_item.setFlags(speed_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.operations_table.setItem(row, 3, speed_item)
        
        # Progress bar
        progress_bar = QProgressBar()
        progress_bar.setMinimum(0)
        progress_bar.setMaximum(100)
        progress_bar.setValue(0)
        progress_bar.setTextVisible(True)
        progress_bar.setFormat("%p%")
        self.operations_table.setCellWidget(row, 4, progress_bar)
        
        # Status label
        status_item = QTableWidgetItem(status)
        status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.operations_table.setItem(row, 5, status_item)
        
        # Action buttons layout
        action_widget = QWidget()
        action_layout = QHBoxLayout(action_widget)
        action_layout.setContentsMargins(1, 1, 1, 1)
        action_layout.setSpacing(2)
        
        # Cancel button
        cancel_button = QPushButton()
        cancel_button.setIcon(QIcon.fromTheme("process-stop"))
        cancel_button.setToolTip("Cancel")
        cancel_button.setFixedSize(24, 24)
        cancel_button.clicked.connect(lambda: self.cancel_operation(operation_id))
        action_layout.addWidget(cancel_button)
        
        # Details button
        details_button = QPushButton()
        details_button.setIcon(QIcon.fromTheme("dialog-information"))
        details_button.setToolTip("Details")
        details_button.setFixedSize(24, 24)
        details_button.clicked.connect(lambda: self.show_operation_details(operation_id=operation_id))
        action_layout.addWidget(details_button)
        
        self.operations_table.setCellWidget(row, 6, action_widget)
        
        # Store operation data
        self.operations[operation_id] = {
            'row': row,
            'type': operation_type,
            'status': status,
            'progress': 0,
            'file_path': file_path,
            'file_size': file_size,
            'start_time': QDateTime.currentDateTime(),
            'end_time': None,
            'state': 'active',  # active, completed, failed
            'details': {},
            'error': None
        }
        
        # Initialize speed tracking
        self.speed_tracking[operation_id] = {
            'last_update_time': time.time(),
            'last_progress': 0,
            'speed': 0
        }
        
        # Update counters
        self.operation_counters['active'] += 1
        self.update_stats()
        
        logger.info(f"Added operation {operation_id}: {operation_type} - {status}")
        
        return operation_id
    
    def update_progress(self, operation_id: str, progress: int, status: str = None):
        """Update progress and optionally status for an operation."""
        if operation_id not in self.operations:
            logger.warning(f"Attempted to update unknown operation: {operation_id}")
            return
            
        operation = self.operations[operation_id]
        operation['progress'] = progress
        
        if status:
            operation['status'] = status
            status_item = self.operations_table.item(operation['row'], 5)
            if status_item:
                status_item.setText(status)
        
        # Update progress bar
        progress_bar = self.operations_table.cellWidget(operation['row'], 4)
        if progress_bar:
            progress_bar.setValue(progress)
        
        # Update speed tracking
        current_time = time.time()
        track_info = self.speed_tracking.get(operation_id, {})
        last_time = track_info.get('last_update_time', current_time)
        last_progress = track_info.get('last_progress', 0)
        
        # Only update if time has passed
        if current_time > last_time and operation.get('file_size'):
            # Calculate bytes transferred since last update
            last_bytes = (last_progress / 100.0) * operation['file_size']
            current_bytes = (progress / 100.0) * operation['file_size']
            bytes_delta = current_bytes - last_bytes
            
            # Calculate time delta
            time_delta = current_time - last_time
            
            if time_delta > 0:
                # Calculate speed in bytes per second
                speed = bytes_delta / time_delta
                track_info['speed'] = speed
                
                # Update speed display
                speed_item = self.operations_table.item(operation['row'], 3)
                if speed_item:
                    speed_item.setText(self.format_speed(speed))
        
        # Update tracking info
        track_info['last_update_time'] = current_time
        track_info['last_progress'] = progress
        self.speed_tracking[operation_id] = track_info
    
    def update_speeds(self):
        """Update all operation speeds."""
        current_time = time.time()
        
        for operation_id, operation in self.operations.items():
            # Skip completed operations
            if operation.get('state') != 'active':
                continue
                
            # Skip operations without file size
            if not operation.get('file_size'):
                continue
                
            track_info = self.speed_tracking.get(operation_id, {})
            last_time = track_info.get('last_update_time', current_time)
            
            # If more than 2 seconds have passed without an update, show 0 speed
            if current_time - last_time > 2:
                speed_item = self.operations_table.item(operation['row'], 3)
                if speed_item:
                    speed_item.setText("")
    
    def complete_operation(self, operation_id: str, success: bool = True, error_message: str = None):
        """Mark an operation as completed or failed."""
        if operation_id not in self.operations:
            logger.warning(f"Attempted to complete unknown operation: {operation_id}")
            return
            
        operation = self.operations[operation_id]
        operation['end_time'] = QDateTime.currentDateTime()
        
        if success:
            operation['state'] = 'completed'
            self.operation_counters['active'] -= 1
            self.operation_counters['completed'] += 1
            
            # Update UI to show completion
            status_item = self.operations_table.item(operation['row'], 5)
            if status_item:
                status_item.setText("Completed")
                status_item.setForeground(QColor(0, 128, 0))  # Green color
            
            # Update progress to 100% if not already
            progress_bar = self.operations_table.cellWidget(operation['row'], 4)
            if progress_bar and progress_bar.value() < 100:
                progress_bar.setValue(100)
                
            logger.info(f"Operation {operation_id} completed successfully")
            
        else:
            operation['state'] = 'failed'
            operation['error'] = error_message or "Operation failed"
            self.operation_counters['active'] -= 1
            self.operation_counters['failed'] += 1
            
            # Update UI to show failure
            status_item = self.operations_table.item(operation['row'], 5)
            if status_item:
                status_text = error_message or "Failed"
                status_item.setText(status_text)
                status_item.setForeground(QColor(255, 0, 0))  # Red color
                
            logger.error(f"Operation {operation_id} failed: {error_message}")
        
        # Clear speed display
        speed_item = self.operations_table.item(operation['row'], 3)
        if speed_item:
            speed_item.setText("")
        
        # Replace cancel button with just the details button
        action_widget = QWidget()
        action_layout = QHBoxLayout(action_widget)
        action_layout.setContentsMargins(1, 1, 1, 1)
        
        # Details button only
        details_button = QPushButton()
        details_button.setIcon(QIcon.fromTheme("dialog-information"))
        details_button.setToolTip("Details")
        details_button.setFixedSize(24, 24)
        details_button.clicked.connect(lambda: self.show_operation_details(operation_id=operation_id))
        action_layout.addWidget(details_button)
        
        self.operations_table.setCellWidget(operation['row'], 6, action_widget)
        
        # Update stats
        self.update_stats()
        
        # Schedule automatic removal if configured
        auto_remove_after = config.get('completed_operations_ttl', 0)
        if auto_remove_after > 0:
            timer = QTimer(self)
            timer.setSingleShot(True)
            timer.timeout.connect(lambda: self.remove_operation(operation_id))
            timer.start(auto_remove_after * 1000)  # Convert seconds to ms
            self.completed_timers[operation_id] = timer
    
    def cancel_operation(self, operation_id: str):
        """Cancel an operation."""
        if operation_id not in self.operations:
            return
            
        operation = self.operations[operation_id]
        
        # Only cancel active operations
        if operation.get('state') != 'active':
            return
            
        logger.info(f"Cancelling operation {operation_id}")
        
        # Update status
        status_item = self.operations_table.item(operation['row'], 5)
        if status_item:
            status_item.setText("Cancelling...")
            status_item.setForeground(QColor(255, 165, 0))  # Orange color
        
        # Emit signal for the worker to cancel
        self.operation_cancelled.emit(operation_id)
        
        # The operation will be marked as failed when the worker completes
    
    def cancel_all_operations(self):
        """Cancel all active operations."""
        active_count = sum(1 for op in self.operations.values() if op.get('state') == 'active')
        
        if active_count == 0:
            return
            
        # Confirm with the user
        result = QMessageBox.question(
            self,
            "Cancel Operations",
            f"Are you sure you want to cancel all {active_count} active operations?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if result != QMessageBox.StandardButton.Yes:
            return
            
        logger.info(f"Cancelling all {active_count} active operations")
        
        # Cancel each active operation
        for operation_id, operation in self.operations.items():
            if operation.get('state') == 'active':
                self.cancel_operation(operation_id)
    
    def clear_completed(self):
        """Clear all completed operations."""
        completed_ids = [op_id for op_id, op in self.operations.items() 
                         if op.get('state') in ('completed', 'failed')]
        
        if not completed_ids:
            return
            
        # Remove operations from bottom to top to avoid row index issues
        operations_to_remove = [(self.operations[op_id]['row'], op_id) for op_id in completed_ids]
        operations_to_remove.sort(reverse=True)  # Sort by row in descending order
        
        for row, op_id in operations_to_remove:
            self.remove_operation(op_id)
    
    def remove_operation(self, operation_id: str):
        """Remove an operation from the table."""
        if operation_id not in self.operations:
            return
            
        operation = self.operations[operation_id]
        
        # Remove from UI
        self.operations_table.removeRow(operation['row'])
        
        # Update row indices for operations after this one
        for op_id, op in self.operations.items():
            if op['row'] > operation['row']:
                op['row'] -= 1
        
        # Clean up timers
        if operation_id in self.completed_timers:
            self.completed_timers[operation_id].stop()
            del self.completed_timers[operation_id]
            
        # Clean up speed tracking
        if operation_id in self.speed_tracking:
            del self.speed_tracking[operation_id]
        
        # Remove from operations dict
        del self.operations[operation_id]
        
        logger.debug(f"Removed operation {operation_id}")
    
    def auto_cleanup(self):
        """Automatically clean up old completed operations."""
        if not config.get('auto_cleanup_enabled', True):
            return
            
        max_age = config.get('auto_cleanup_age', 10) * 60  # Convert to seconds
        current_time = QDateTime.currentDateTime()
        
        operations_to_remove = []
        
        for operation_id, operation in self.operations.items():
            if operation.get('state') in ('completed', 'failed') and operation.get('end_time'):
                age_secs = operation['end_time'].secsTo(current_time)
                if age_secs > max_age:
                    operations_to_remove.append(operation_id)
        
        if operations_to_remove:
            logger.info(f"Auto-cleaning {len(operations_to_remove)} old operations")
            
            # Sort by row descending
            operations_to_remove = [(self.operations[op_id]['row'], op_id) for op_id in operations_to_remove]
            operations_to_remove.sort(reverse=True)
            
            for row, op_id in operations_to_remove:
                self.remove_operation(op_id)
    
    def update_stats(self):
        """Update the operation statistics display."""
        self.active_count_label.setText(f"Active: {self.operation_counters['active']}")
        self.completed_count_label.setText(f"Completed: {self.operation_counters['completed']}")
        self.failed_count_label.setText(f"Failed: {self.operation_counters['failed']}")
    
    def show_operation_details(self, index=None, operation_id=None):
        """Show details dialog for an operation."""
        if operation_id is None and index is not None:
            # Find the operation ID for the clicked row
            row = index.row()
            for op_id, op in self.operations.items():
                if op['row'] == row:
                    operation_id = op_id
                    break
        
        if operation_id is None or operation_id not in self.operations:
            return
            
        # Create and show the details dialog
        dialog = OperationDetailsDialog(self.operations[operation_id], self)
        dialog.exec()
    
    def closeEvent(self, event):
        """Handle window close event."""
        # Just hide the window instead of closing it
        self.hide()
        event.ignore() 