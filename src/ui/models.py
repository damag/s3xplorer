from PyQt6.QtCore import Qt, QAbstractItemModel, QModelIndex, QAbstractTableModel
from typing import List, Dict, Any, Optional
from datetime import datetime
import os

class BucketTreeModel(QAbstractItemModel):
    def __init__(self, buckets: List[Dict[str, Any]]):
        super().__init__()
        self.buckets = buckets
        self.root_item = {"name": "root", "children": []}
        self.setup_model()
    
    def setup_model(self):
        """Setup the tree model with buckets and their contents."""
        for bucket in self.buckets:
            bucket_item = {
                "name": bucket["Name"],
                "children": []
            }
            self.root_item["children"].append(bucket_item)
    
    def index(self, row: int, column: int, parent: QModelIndex = QModelIndex()) -> QModelIndex:
        """Create an index for the given row and column."""
        if not self.hasIndex(row, column, parent):
            return QModelIndex()
        
        if not parent.isValid():
            parent_item = self.root_item
        else:
            parent_item = parent.internalPointer()
        
        if row < len(parent_item["children"]):
            return self.createIndex(row, column, parent_item["children"][row])
        return QModelIndex()
    
    def parent(self, index: QModelIndex) -> QModelIndex:
        """Get the parent index of the given index."""
        if not index.isValid():
            return QModelIndex()
        
        child_item = index.internalPointer()
        parent_item = self.get_parent_item(child_item)
        
        if parent_item == self.root_item:
            return QModelIndex()
        
        return self.createIndex(self.get_row(parent_item), 0, parent_item)
    
    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Get the number of rows under the given parent."""
        if parent.column() > 0:
            return 0
        
        if not parent.isValid():
            parent_item = self.root_item
        else:
            parent_item = parent.internalPointer()
        
        return len(parent_item["children"])
    
    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Get the number of columns."""
        return 1
    
    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        """Get the data for the given index and role."""
        if not index.isValid():
            return None
        
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        
        item = index.internalPointer()
        return item["name"]
    
    def get_parent_item(self, child_item: Dict) -> Dict:
        """Get the parent item of the given child item."""
        for bucket in self.root_item["children"]:
            if bucket == child_item:
                return self.root_item
            if "children" in bucket:
                for child in bucket["children"]:
                    if child == child_item:
                        return bucket
        return self.root_item
    
    def get_row(self, item: Dict) -> int:
        """Get the row number of the given item."""
        parent = self.get_parent_item(item)
        return parent["children"].index(item)

class ObjectTableModel(QAbstractTableModel):
    def __init__(self, objects: List[Dict[str, Any]]):
        super().__init__()
        self.objects = objects
        self.headers = ["Name", "Size", "Last Modified", "Storage Class"]
    
    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Get the number of rows."""
        return len(self.objects)
    
    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Get the number of columns."""
        return len(self.headers)
    
    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        """Get the data for the given index and role."""
        if not index.isValid():
            return None
        
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        
        obj = self.objects[index.row()]
        
        if index.column() == 0:  # Name
            return obj["Key"]
        elif index.column() == 1:  # Size
            return self.format_size(obj["Size"])
        elif index.column() == 2:  # Last Modified
            return obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
        elif index.column() == 3:  # Storage Class
            return obj.get("StorageClass", "STANDARD")
        
        return None
    
    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        """Get the header data for the given section and orientation."""
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        
        if orientation == Qt.Orientation.Horizontal:
            return self.headers[section]
        
        return None
    
    def format_size(self, size: int) -> str:
        """Format the size in bytes to a human-readable string."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024:
                return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} PB"

class S3ObjectTreeModel(QAbstractItemModel):
    """Tree model for displaying S3 objects in a hierarchical structure."""
    
    def __init__(self, objects: List[Dict[str, Any]], parent=None, directories_only=False):
        super().__init__(parent)
        self.objects = objects
        self.directories_only = directories_only
        self.root_item = {"name": "root", "path": "", "type": "directory", "children": [], "size": 0, "data": None}
        
        # Add "Root" node for bucket root
        self.root_node = {"name": "/", "path": "", "type": "directory", "children": [], "size": 0, "data": None}
        self.root_item["children"].append(self.root_node)
        
        self.setup_model()
    
    def setup_model(self):
        """Setup the tree model with folders and files."""
        # Process each S3 object
        for obj in self.objects:
            # Skip objects ending with / (directory markers)
            if obj["Key"].endswith("/") and len(obj["Key"]) > 1:
                self._add_directory_marker(obj["Key"], obj)
                continue
                
            path_parts = obj["Key"].split("/")
            
            # Handle objects in root directory
            if len(path_parts) == 1:
                if not self.directories_only:  # Skip files in directories-only mode
                    self.root_node["children"].append({
                        "name": path_parts[0],
                        "path": obj["Key"],
                        "type": "file",
                        "children": [],
                        "size": obj["Size"],
                        "data": obj
                    })
                continue
            
            # Handle objects in subdirectories
            current = self.root_node
            for i, part in enumerate(path_parts):
                if i == len(path_parts) - 1:  # Last part (file)
                    if not self.directories_only:  # Skip files in directories-only mode
                        current["children"].append({
                            "name": part,
                            "path": obj["Key"],
                            "type": "file",
                            "children": [],
                            "size": obj["Size"],
                            "data": obj
                        })
                else:  # Directory
                    # Find or create directory
                    dir_found = False
                    for child in current["children"]:
                        if child["name"] == part and child["type"] == "directory":
                            current = child
                            dir_found = True
                            break
                    
                    if not dir_found:
                        new_dir = {
                            "name": part,
                            "path": "/".join(path_parts[:i+1]) + "/",
                            "type": "directory",
                            "children": [],
                            "size": 0,
                            "data": None
                        }
                        current["children"].append(new_dir)
                        current = new_dir
    
    def _add_directory_marker(self, key: str, obj: Dict[str, Any]):
        """Add directory marker to the tree."""
        path_parts = key.split("/")
        path_parts = [part for part in path_parts if part]  # Remove empty parts
        
        # Handle empty directory marker
        if not path_parts:
            return
            
        current = self.root_node
        for i, part in enumerate(path_parts):
            # Find or create directory
            dir_found = False
            for child in current["children"]:
                if child["name"] == part and child["type"] == "directory":
                    current = child
                    dir_found = True
                    break
            
            if not dir_found:
                new_dir = {
                    "name": part,
                    "path": "/".join(path_parts[:i+1]) + "/",
                    "type": "directory",
                    "children": [],
                    "size": 0,
                    "data": obj if i == len(path_parts) - 1 else None
                }
                current["children"].append(new_dir)
                current = new_dir
    
    def index(self, row: int, column: int, parent: QModelIndex = QModelIndex()) -> QModelIndex:
        """Create an index for the given row and column."""
        if not self.hasIndex(row, column, parent):
            return QModelIndex()
        
        if not parent.isValid():
            parent_item = self.root_item
        else:
            parent_item = parent.internalPointer()
        
        if row < len(parent_item["children"]):
            return self.createIndex(row, column, parent_item["children"][row])
        return QModelIndex()
    
    def parent(self, index: QModelIndex) -> QModelIndex:
        """Get the parent index of the given index."""
        if not index.isValid():
            return QModelIndex()
        
        child_item = index.internalPointer()
        parent_item = self._get_parent_item(child_item)
        
        if parent_item == self.root_item:
            return QModelIndex()
        
        return self.createIndex(self._get_row(parent_item), 0, parent_item)
    
    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Get the number of rows under the given parent."""
        if parent.column() > 0:
            return 0
        
        if not parent.isValid():
            parent_item = self.root_item
        else:
            parent_item = parent.internalPointer()
        
        return len(parent_item["children"])
    
    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Get the number of columns."""
        return 1
    
    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        """Get the data for the given index and role."""
        if not index.isValid():
            return None
        
        item = index.internalPointer()
        
        if role == Qt.ItemDataRole.DisplayRole:
            if item["type"] == "directory":
                # Don't add a slash to the root directory since it already has one
                if item["name"] == "/":
                    return item["name"]
                return f"{item['name']}/"
            else:
                return item["name"]
        
        elif role == Qt.ItemDataRole.DecorationRole:
            if item["type"] == "directory":
                return None  # You can add an icon for folders here if desired
            else:
                return None  # You can add an icon for files here if desired
                
        return None
    
    def _get_parent_item(self, item: Dict) -> Dict:
        """Get the parent item of the given item."""
        def find_parent(parent, item):
            for child in parent["children"]:
                if child == item:
                    return parent
                if "children" in child:
                    result = find_parent(child, item)
                    if result:
                        return result
            return None
        
        result = find_parent(self.root_item, item)
        return result if result else self.root_item
    
    def _get_row(self, item: Dict) -> int:
        """Get the row number of the given item."""
        parent = self._get_parent_item(item)
        if parent and "children" in parent:
            return parent["children"].index(item)
        return 0
    
    def get_item_path(self, index: QModelIndex) -> str:
        """Get the full path of the item at the given index."""
        if not index.isValid():
            return ""
        
        item = index.internalPointer()
        return item["path"]
    
    def get_item_type(self, index: QModelIndex) -> str:
        """Get the type of the item at the given index."""
        if not index.isValid():
            return ""
        
        item = index.internalPointer()
        return item["type"]
    
    def get_item_data(self, index: QModelIndex) -> Optional[Dict]:
        """Get the object data of the item at the given index."""
        if not index.isValid():
            return None
        
        item = index.internalPointer()
        return item["data"] 