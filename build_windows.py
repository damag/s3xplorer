import os
import shutil
import subprocess
from pathlib import Path

def clean_build():
    """Clean previous build artifacts"""
    dirs_to_clean = ['build', 'dist']
    for dir_name in dirs_to_clean:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
    
    # Clean spec file if it exists
    spec_file = 's3xplorer.spec'
    if os.path.exists(spec_file):
        os.remove(spec_file)

def build_executable():
    """Build the Windows executable using PyInstaller"""
    # Get the absolute path to the resources directory
    resources_dir = os.path.abspath('resources')
    
    # PyInstaller command with all necessary options
    cmd = [
        'pyinstaller',
        '--name=s3xplorer',
        '--onefile',
        '--windowed',
        '--icon=resources/icon.ico',  # Make sure to add an icon file
        f'--add-data={resources_dir};resources',
        '--clean',
        'run.py'
    ]
    
    # Run PyInstaller
    subprocess.run(cmd, check=True)
    
    print("\nBuild completed successfully!")
    print(f"Executable can be found in the 'dist' directory")

if __name__ == '__main__':
    print("Cleaning previous build artifacts...")
    clean_build()
    
    print("\nBuilding Windows executable...")
    build_executable() 