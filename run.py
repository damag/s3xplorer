#!/usr/bin/env python3
"""
S3xplorer - A cross-platform S3 client desktop application
"""

import sys
import os

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from src.main import main

if __name__ == "__main__":
    main() 