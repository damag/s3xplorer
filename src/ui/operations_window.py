from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                             QLabel, QProgressBar, QTableWidget, QTableWidgetItem,
                             QHeaderView)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QDateTime
from PyQt6.QtGui import QIcon
import uuid
import os

class OperationsWindow(QWidget):
    """Window to display ongoing operations."""
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
        
        # Create table for operations
        self.operations_table = QTableWidget()
        self.operations_table.setColumnCount(7)
        self.operations_table.setHorizontalHeaderLabels([
            "Operation", "File", "Size", "Speed", "Progress", "Status", ""
        ])
        self.operations_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.operations_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeMode.Fixed)
        self.operations_table.horizontalHeader().setSectionResizeMode(6, QHeaderView.ResizeMode.Fixed)
        
        # Set column widths
        self.operations_table.setColumnWidth(0, 100)  # Operation
        self.operations_table.setColumnWidth(2, 100)  # Size
        self.operations_table.setColumnWidth(3, 100)  # Speed
        self.operations_table.setColumnWidth(4, 200)  # Progress
        self.operations_table.setColumnWidth(5, 100)  # Status
        self.operations_table.setColumnWidth(6, 40)   # Cancel button
        
        self.operations_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.operations_table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        layout.addWidget(self.operations_table)
        
        # Add close button at the bottom
        close_button = QPushButton("Close")
        close_button.clicked.connect(self.hide)
        close_button.setFixedWidth(100)
        
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        button_layout.addWidget(close_button)
        layout.addLayout(button_layout)
        
        # Store operation data
        self.operations = {}
        self.completed_timers = {}
        self.speed_timers = {}
        self.last_progress = {}
    
    def format_size(self, size_bytes):
        """Format size in bytes to human readable string."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"
    
    def format_speed(self, bytes_per_second):
        """Format speed in bytes per second to human readable string."""
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
        status_item = QTableWidgetItem("In Progress")
        status_item.setFlags(status_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
        self.operations_table.setItem(row, 5, status_item)
        
        # Cancel button with icon
        cancel_button = QPushButton()
        cancel_button.setIcon(QIcon.fromTheme("process-stop"))
        cancel_button.setToolTip("Cancel")
        cancel_button.setFixedSize(24, 24)
        cancel_button.clicked.connect(lambda: self.cancel_operation(operation_id))
        self.operations_table.setCellWidget(row, 6, cancel_button)
        
        # Store operation data
        self.operations[operation_id] = {
            'row': row,
            'type': operation_type,
            'status': status,
            'progress': 0,
            'file_path': file_path,
            'file_size': file_size,
            'start_time': QDateTime.currentDateTime()
        }
        
        # Initialize speed tracking
        self.last_progress[operation_id] = 0
        
        return operation_id
    
    def update_progress(self, operation_id: str, progress: int):
        """Update progress for an operation."""
        if operation_id not in self.operations:
            return
            
        operation = self.operations[operation_id]
        operation['progress'] = progress
        
        # Update progress bar
        progress_bar = self.operations_table.cellWidget(operation['row'], 4)
        if progress_bar:
            progress_bar.setValue(progress)
        
        # Calculate and update speed
        if operation['file_size']:
            elapsed = operation['start_time'].msecsTo(QDateTime.currentDateTime()) / 1000.0
            if elapsed > 0:
                bytes_per_second = (progress / 100.0 * operation['file_size'] - self.last_progress[operation_id]) / elapsed
                speed_item = self.operations_table.item(operation['row'], 3)
                if speed_item:
                    speed_item.setText(self.format_speed(bytes_per_second))
        
        self.last_progress[operation_id] = progress
    
    def complete_operation(self, operation_id: str):
        """Mark an operation as completed."""
        if operation_id not in self.operations:
            return
            
        operation = self.operations[operation_id]
        
        # Update progress to 100%
        progress_bar = self.operations_table.cellWidget(operation['row'], 4)
        if progress_bar:
            progress_bar.setValue(100)
        
        # Update status
        status_item = self.operations_table.item(operation['row'], 5)
        if status_item:
            status_item.setText("Completed")
            status_item.setForeground(Qt.GlobalColor.green)
        
        # Remove cancel button
        self.operations_table.removeCellWidget(operation['row'], 6)
        
        # Clear speed
        speed_item = self.operations_table.item(operation['row'], 3)
        if speed_item:
            speed_item.setText("")
        
        # Create timer to remove the operation after delay
        timer = QTimer(self)
        timer.setSingleShot(True)
        timer.timeout.connect(lambda: self.remove_operation(operation_id))
        self.completed_timers[operation_id] = timer
        timer.start(3000)  # Remove after 3 seconds
    
    def cancel_operation(self, operation_id: str):
        """Cancel an operation."""
        if operation_id not in self.operations:
            return
            
        operation = self.operations[operation_id]
        
        # Update status
        status_item = self.operations_table.item(operation['row'], 5)
        if status_item:
            status_item.setText("Cancelled")
            status_item.setForeground(Qt.GlobalColor.red)
        
        # Remove cancel button
        self.operations_table.removeCellWidget(operation['row'], 6)
        
        # Clear speed
        speed_item = self.operations_table.item(operation['row'], 3)
        if speed_item:
            speed_item.setText("")
        
        # Emit cancellation signal
        self.operation_cancelled.emit(operation_id)
        
        # Create timer to remove the operation after delay
        timer = QTimer(self)
        timer.setSingleShot(True)
        timer.timeout.connect(lambda: self.remove_operation(operation_id))
        self.completed_timers[operation_id] = timer
        timer.start(3000)  # Remove after 3 seconds
    
    def remove_operation(self, operation_id: str):
        """Remove an operation from the table."""
        if operation_id in self.operations:
            row = self.operations[operation_id]['row']
            self.operations_table.removeRow(row)
            del self.operations[operation_id]
            
            # Update row numbers for remaining operations
            for op in self.operations.values():
                if op['row'] > row:
                    op['row'] -= 1
        
        # Clean up timer
        if operation_id in self.completed_timers:
            self.completed_timers[operation_id].deleteLater()
            del self.completed_timers[operation_id]
    
    def closeEvent(self, event):
        """Handle window close event."""
        event.ignore()
        self.hide() 