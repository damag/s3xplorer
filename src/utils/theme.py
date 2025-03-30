"""Theme management for the application UI."""

from PyQt6.QtGui import QPalette, QColor, QFont
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt
from typing import Dict, Any

from src.utils import get_logger, get_config

logger = get_logger()
config = get_config()

class ThemeManager:
    """Manages application themes and styling."""
    
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get or create singleton instance."""
        if cls._instance is None:
            cls._instance = ThemeManager()
        return cls._instance
    
    def __init__(self):
        """Initialize the theme manager."""
        # Define available themes
        self.themes = {
            "default": {
                "name": "Default Light",
                "description": "Default light theme",
                "is_dark": False
            },
            "dark": {
                "name": "Dark",
                "description": "Modern dark theme",
                "is_dark": True
            },
            "blue": {
                "name": "Blue",
                "description": "Professional blue theme",
                "is_dark": False
            },
            "high_contrast": {
                "name": "High Contrast",
                "description": "High contrast theme for better visibility",
                "is_dark": True
            }
        }
        
        # Load current theme from config
        self.current_theme = config.get("theme", "default")
        
        # Define style sheets and palettes for each theme
        self._init_themes()
    
    def _init_themes(self):
        """Initialize theme styles and palettes."""
        # Default light theme
        self.themes["default"]["stylesheet"] = """
            QMainWindow, QDialog {
                background-color: #f5f5f5;
            }
            
            QToolBar, QStatusBar {
                background-color: #e0e0e0;
                border: none;
            }
            
            QPushButton {
                background-color: #f0f0f0;
                border: 1px solid #cccccc;
                border-radius: 4px;
                padding: 4px 8px;
            }
            
            QPushButton:hover {
                background-color: #e3e3e3;
            }
            
            QPushButton:pressed {
                background-color: #d0d0d0;
            }
            
            QTableWidget {
                gridline-color: #d0d0d0;
                background-color: white;
                selection-background-color: #d7e8f8;
                selection-color: black;
            }
            
            QTableWidget::item {
                padding: 4px;
            }
            
            QHeaderView::section {
                background-color: #e0e0e0;
                padding: 4px;
                border: none;
                border-right: 1px solid #c0c0c0;
                border-bottom: 1px solid #c0c0c0;
            }
            
            QTreeView {
                background-color: white;
                selection-background-color: #d7e8f8;
                selection-color: black;
            }
            
            QProgressBar {
                border: 1px solid #cccccc;
                border-radius: 4px;
                text-align: center;
                background-color: #f0f0f0;
            }
            
            QProgressBar::chunk {
                background-color: #4d94ff;
                width: 1px;
            }
            
            QListWidget {
                background-color: white;
                selection-background-color: #d7e8f8;
                selection-color: black;
            }
            
            QSplitter::handle {
                background-color: #e0e0e0;
            }
            
            QMessageBox {
                background-color: #f5f5f5;
            }
            
            QMenuBar {
                background-color: #f0f0f0;
                border-bottom: 1px solid #d0d0d0;
            }
            
            QMenuBar::item:selected {
                background-color: #d7e8f8;
            }
            
            QMenu {
                background-color: white;
                border: 1px solid #d0d0d0;
            }
            
            QMenu::item:selected {
                background-color: #d7e8f8;
            }
        """
        
        # Dark theme
        self.themes["dark"]["stylesheet"] = """
            QMainWindow, QDialog {
                background-color: #2d2d2d;
                color: #ffffff;
            }
            
            QToolBar, QStatusBar {
                background-color: #363636;
                color: #ffffff;
                border: none;
            }
            
            QPushButton {
                background-color: #444444;
                color: #ffffff;
                border: 1px solid #555555;
                border-radius: 4px;
                padding: 4px 8px;
            }
            
            QPushButton:hover {
                background-color: #505050;
            }
            
            QPushButton:pressed {
                background-color: #353535;
            }
            
            QLabel {
                color: #ffffff;
            }
            
            QTableWidget {
                background-color: #3d3d3d;
                color: #ffffff;
                gridline-color: #505050;
                selection-background-color: #2a617c;
                selection-color: #ffffff;
                border: 1px solid #505050;
            }
            
            QTableWidget::item {
                padding: 4px;
            }
            
            QHeaderView::section {
                background-color: #444444;
                color: #ffffff;
                padding: 4px;
                border: none;
                border-right: 1px solid #505050;
                border-bottom: 1px solid #505050;
            }
            
            QTreeView {
                background-color: #3d3d3d;
                color: #ffffff;
                selection-background-color: #2a617c;
                selection-color: #ffffff;
                border: 1px solid #505050;
            }
            
            QProgressBar {
                border: 1px solid #505050;
                border-radius: 4px;
                text-align: center;
                background-color: #3d3d3d;
                color: #ffffff;
            }
            
            QProgressBar::chunk {
                background-color: #2a82da;
                width: 1px;
            }
            
            QListWidget {
                background-color: #3d3d3d;
                color: #ffffff;
                selection-background-color: #2a617c;
                selection-color: #ffffff;
                border: 1px solid #505050;
            }
            
            QSplitter::handle {
                background-color: #444444;
            }
            
            QLineEdit, QTextEdit, QPlainTextEdit, QComboBox {
                background-color: #3d3d3d;
                color: #ffffff;
                border: 1px solid #505050;
                border-radius: 4px;
                padding: 2px;
            }
            
            QComboBox:drop-down {
                border: 0px;
            }
            
            QComboBox:down-arrow {
                width: 14px;
                height: 14px;
            }
            
            QMenuBar {
                background-color: #363636;
                color: #ffffff;
                border-bottom: 1px solid #505050;
            }
            
            QMenuBar::item:selected {
                background-color: #2a617c;
            }
            
            QMenu {
                background-color: #3d3d3d;
                color: #ffffff;
                border: 1px solid #505050;
            }
            
            QMenu::item:selected {
                background-color: #2a617c;
            }
            
            QMessageBox {
                background-color: #2d2d2d;
                color: #ffffff;
            }
        """
        
        # Blue theme
        self.themes["blue"]["stylesheet"] = """
            QMainWindow, QDialog {
                background-color: #f0f4f8;
            }
            
            QToolBar, QStatusBar {
                background-color: #e1e8f0;
                border: none;
            }
            
            QPushButton {
                background-color: #4a89dc;
                color: white;
                border: 1px solid #3a79cc;
                border-radius: 4px;
                padding: 4px 8px;
                font-weight: bold;
            }
            
            QPushButton:hover {
                background-color: #5a99ec;
            }
            
            QPushButton:pressed {
                background-color: #3a79cc;
            }
            
            QTableWidget {
                gridline-color: #c0d0e0;
                background-color: white;
                selection-background-color: #c3daf9;
                selection-color: black;
            }
            
            QTableWidget::item {
                padding: 4px;
            }
            
            QHeaderView::section {
                background-color: #e1e8f0;
                padding: 4px;
                border: none;
                border-right: 1px solid #c0d0e0;
                border-bottom: 1px solid #c0d0e0;
                font-weight: bold;
            }
            
            QTreeView {
                background-color: white;
                selection-background-color: #c3daf9;
                selection-color: black;
            }
            
            QProgressBar {
                border: 1px solid #c0d0e0;
                border-radius: 4px;
                text-align: center;
                background-color: #f0f4f8;
            }
            
            QProgressBar::chunk {
                background-color: #4a89dc;
                width: 1px;
            }
            
            QListWidget {
                background-color: white;
                selection-background-color: #c3daf9;
                selection-color: black;
            }
            
            QSplitter::handle {
                background-color: #e1e8f0;
            }
        """
        
        # High contrast theme
        self.themes["high_contrast"]["stylesheet"] = """
            QMainWindow, QDialog {
                background-color: #000000;
                color: #ffffff;
            }
            
            QToolBar, QStatusBar {
                background-color: #000000;
                color: #ffffff;
                border: 1px solid #ffffff;
            }
            
            QPushButton {
                background-color: #000000;
                color: #ffffff;
                border: 2px solid #ffffff;
                border-radius: 4px;
                padding: 4px 8px;
                font-weight: bold;
            }
            
            QPushButton:hover {
                background-color: #333333;
            }
            
            QPushButton:pressed {
                background-color: #ffffff;
                color: #000000;
            }
            
            QLabel {
                color: #ffffff;
                font-size: 12pt;
            }
            
            QTableWidget {
                background-color: #000000;
                color: #ffffff;
                gridline-color: #ffffff;
                selection-background-color: #ffffff;
                selection-color: #000000;
                border: 2px solid #ffffff;
            }
            
            QTableWidget::item {
                padding: 6px;
                border-bottom: 1px solid #444444;
            }
            
            QHeaderView::section {
                background-color: #000000;
                color: #ffffff;
                padding: 6px;
                border: 2px solid #ffffff;
                font-weight: bold;
                font-size: 12pt;
            }
            
            QTreeView {
                background-color: #000000;
                color: #ffffff;
                selection-background-color: #ffffff;
                selection-color: #000000;
                border: 2px solid #ffffff;
            }
            
            QProgressBar {
                border: 2px solid #ffffff;
                border-radius: 4px;
                text-align: center;
                background-color: #000000;
                color: #ffffff;
                font-weight: bold;
            }
            
            QProgressBar::chunk {
                background-color: #ffffff;
                width: 1px;
            }
            
            QListWidget {
                background-color: #000000;
                color: #ffffff;
                selection-background-color: #ffffff;
                selection-color: #000000;
                border: 2px solid #ffffff;
            }
            
            QLineEdit, QTextEdit, QPlainTextEdit, QComboBox {
                background-color: #000000;
                color: #ffffff;
                border: 2px solid #ffffff;
                border-radius: 4px;
                padding: 4px;
                font-size: 12pt;
            }
        """
    
    def get_themes(self) -> Dict[str, Dict[str, Any]]:
        """Get all available themes."""
        return self.themes
    
    def apply_theme(self, theme_id: str):
        """Apply a theme to the application."""
        if theme_id not in self.themes:
            logger.error(f"Theme '{theme_id}' not found")
            return False
        
        logger.info(f"Applying theme: {self.themes[theme_id]['name']}")
        
        app = QApplication.instance()
        if not app:
            logger.error("No QApplication instance found")
            return False
        
        # Apply stylesheet
        app.setStyleSheet(self.themes[theme_id]["stylesheet"])
        
        # Save theme to config
        self.current_theme = theme_id
        config.set("theme", theme_id)
        
        return True
    
    def get_current_theme(self) -> str:
        """Get the current theme ID."""
        return self.current_theme

def apply_theme(theme_id: str = None):
    """Apply a theme to the application."""
    manager = ThemeManager.get_instance()
    if theme_id is None:
        theme_id = manager.get_current_theme()
    return manager.apply_theme(theme_id)

def get_theme_manager() -> ThemeManager:
    """Get the theme manager instance."""
    return ThemeManager.get_instance() 