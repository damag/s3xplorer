import sys
import argparse
import os
from pathlib import Path
import traceback

from PyQt6.QtWidgets import QApplication, QMainWindow, QStyleFactory, QMessageBox
from PyQt6.QtCore import Qt, QSize, QSettings, QStandardPaths
from PyQt6.QtGui import QIcon, QFont

from src.ui.main_window import MainWindow
from src.utils import get_logger, get_config, apply_theme

# Configure application-wide logger
logger = get_logger()
config = get_config()

def handle_exception(exc_type, exc_value, exc_traceback):
    """Handle uncaught exceptions by logging them."""
    if issubclass(exc_type, KeyboardInterrupt):
        # Let the default handler take care of KeyboardInterrupt
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
        
    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    
    # Show error message to user
    error_msg = f"An unexpected error occurred:\n{exc_value}"
    if QApplication.instance():
        QMessageBox.critical(None, "Application Error", error_msg)

def main():
    """Application entry point."""
    # Install exception hook
    sys.excepthook = handle_exception
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='S3xplorer - Amazon S3 Client')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('-t', '--theme', choices=['default', 'dark', 'blue', 'high_contrast'], 
                        help='UI theme to use')
    args = parser.parse_args()
    
    # Configure logging level based on verbose flag
    if args.verbose:
        logger.setLevel("DEBUG")
        logger.debug("Verbose mode enabled.")
    
    # Create the application
    app = QApplication(sys.argv)
    
    # Set application metadata
    app.setApplicationName("S3xplorer")
    app.setApplicationVersion("1.1.0")
    app.setOrganizationName("S3xplorer")
    
    # Apply theme (either from args or config)
    theme = args.theme or config.get('theme', 'default')
    apply_theme(theme)
    
    # Set application font (optional)
    if config.get('custom_font_enabled', False):
        font_family = config.get('font_family', 'Arial')
        font_size = config.get('font_size', 10)
        font = QFont(font_family, font_size)
        app.setFont(font)
    
    # Create and show the main window
    window = MainWindow()
    window.show()
    
    # Set verbose mode
    window.set_verbose_mode(args.verbose)
    
    # Clean up temporary files on startup
    from src.utils import get_file_service
    file_service = get_file_service()
    file_service.cleanup_temp_files()
    
    # Start the event loop
    exit_code = app.exec()
    
    # Clean up before exit
    logger.info("Application shutting down")
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main() 