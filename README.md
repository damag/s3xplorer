# S3xplorer

A modern, user-friendly S3 browser application built with PyQt6. This application allows you to browse, upload, download, and manage files in your Amazon S3 buckets with an intuitive graphical interface.

## Features

- Browse S3 buckets and objects
- Upload files to S3
- Download files from S3
- Delete objects from S3
- Real-time operation progress tracking
- Modern and intuitive user interface
- Support for nested directories in S3

## Requirements

- Python 3
- PyQt6
- boto3 (AWS SDK for Python)

## Installation

### From Source

1. Clone the repository:
```bash
git clone https://github.com/damag/s3xplorer.git
cd s3xplorer
```

2. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the required packages:
```bash
pip install -r requirements.txt
```

### Windows Executable

1. Download the latest release from the releases page
2. Extract the zip file
3. Run `s3xplorer.exe`

## Building Windows Executable

To build a standalone Windows executable:

1. Make sure you have all requirements installed:
```bash
pip install -r requirements.txt
```

2. Run the build script:
```bash
python build_windows.py
```

3. The executable will be created in the `dist` directory as `s3xplorer.exe`

Note: Building the executable requires Windows. The resulting executable can be distributed and run on any Windows system without requiring Python installation.

## Usage

1. Run the application:
```bash
python run.py
```

2. Enter your AWS credentials when prompted
3. Start browsing your S3 buckets!

## License

This project is licensed under the MIT License - see the LICENSE file for details.
