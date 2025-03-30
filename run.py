#!/usr/bin/env python3
"""
S3xplorer - Modern Amazon S3 Client
-----------------------------------
A user-friendly S3 browser built with PyQt6.

Run this script to launch the application.
"""

import os
import sys
import subprocess
import traceback
from pathlib import Path

def ensure_path():
    """Ensure the application path is in the Python path."""
    # Get the directory containing this script
    app_dir = Path(__file__).resolve().parent
    
    # Add to Python path if not already there
    if str(app_dir) not in sys.path:
        sys.path.insert(0, str(app_dir))

def ensure_dependencies():
    """Check and install missing dependencies."""
    try:
        # Try importing key dependencies
        import PyQt6
        import boto3
        import botocore
        return True
    except ImportError as e:
        print(f"Missing dependency: {e}")
        
        # Ask user if they want to install dependencies
        response = input("Would you like to install the required dependencies? (y/n): ")
        if response.lower() not in ('y', 'yes'):
            return False
        
        # Install dependencies
        print("Installing dependencies...")
        requirements_file = Path(__file__).resolve().parent / 'requirements.txt'
        if requirements_file.exists():
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', str(requirements_file)])
                print("Dependencies installed successfully.")
                return True
            except subprocess.CalledProcessError:
                print("Failed to install dependencies.")
                return False
        else:
            print("Could not find requirements.txt file.")
            return False

def main():
    """Main entry point."""
    # Ensure path
    ensure_path()
    
    # Check dependencies
    if not ensure_dependencies():
        print("Cannot run S3xplorer due to missing dependencies.")
        return 1
    
    # Import and run the application
    try:
        from src.main import main as app_main
        return app_main()
    except Exception as e:
        print(f"Error starting application: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main()) 