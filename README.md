# S3xplorer

A modern, user-friendly S3 browser application built with PyQt6. This application allows you to browse, upload, download, and manage files in your Amazon S3 buckets with an intuitive graphical interface.

![S3xplorer Screenshot](resources/screenshot.png)

## Features

- Browse S3 buckets and objects
- Upload files to S3
- Download files from S3
- Delete objects from S3
- Real-time operation progress tracking
- Modern and intuitive user interface
- Support for nested directories in S3
- Multiple authentication methods: Access keys, IAM profiles, SSO
- Multiple UI themes with light and dark mode
- Detailed operation history and status tracking
- Pre-signed URL generation for sharing objects
- Multi-part uploads for large files
- Automatic retry mechanism for improved reliability

## Requirements

- Python 3.8 or higher
- PyQt6
- boto3 (AWS SDK for Python)
- Additional dependencies in requirements.txt

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

2. Authentication Options:
   - Enter your AWS access key and secret key
   - Use an existing AWS profile
   - Connect via AWS SSO

3. Navigate your S3 buckets and objects:
   - Use the left panel to select a bucket
   - Use the middle panel to navigate directories
   - Use the right panel to view and manipulate objects

4. Operations:
   - Upload: Right-click in the objects panel or use the upload button
   - Download: Select objects and click download or use right-click menu
   - Delete: Select objects and press Delete or use right-click menu
   - Generate URL: Right-click an object and select "Generate URL"
   - View Properties: Right-click an object and select "Properties"

## Configuration

S3xplorer stores configuration in the user's home directory:
- Linux/macOS: `~/.s3xplorer/`
- Windows: `C:\Users\USERNAME\.s3xplorer\`

You can customize the following settings:
- UI Theme (default, dark, blue, high-contrast)
- Default region
- Concurrent operation limit
- Auto-refresh interval
- Font settings
- Authentication profiles

## Command Line Options

- `-v`, `--verbose`: Enable verbose logging
- `-t`, `--theme`: Specify UI theme (default, dark, blue, high-contrast)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- The AWS team for the boto3 library
- The PyQt team for the Qt Python bindings
