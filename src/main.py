import sys
import argparse
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import Qt
from src.ui.main_window import MainWindow

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='S3 Explorer - AWS S3 Browser')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    
    # Create the application
    app = QApplication(sys.argv)
    
    # Set application metadata
    app.setApplicationName("S3xplorer")
    app.setApplicationVersion("1.0.0")
    app.setOrganizationName("S3xplorer")
    
    # Create and show the main window
    window = MainWindow()
    window.show()
    
    # Set verbose mode
    window.set_verbose_mode(args.verbose)
    
    # Start the event loop
    sys.exit(app.exec())

if __name__ == "__main__":
    main() 