#!/usr/bin/env python3
"""
Test runner for S3xplorer.

This script runs all the tests for the S3xplorer application.
"""

import unittest
import sys
import os
import argparse
from pathlib import Path

def main():
    """Run all tests."""
    # Parse arguments
    parser = argparse.ArgumentParser(description='Run S3xplorer tests')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--pattern', '-p', default='test_*.py', help='Test file pattern')
    args = parser.parse_args()
    
    # Set the verbosity
    verbosity = 2 if args.verbose else 1
    
    # Get the test directory
    test_dir = Path(__file__).resolve().parent / 'tests'
    
    # Discover and run tests
    print(f"Discovering tests in {test_dir}...")
    test_suite = unittest.defaultTestLoader.discover(
        str(test_dir),
        pattern=args.pattern
    )
    
    # Run tests
    test_runner = unittest.TextTestRunner(verbosity=verbosity)
    result = test_runner.run(test_suite)
    
    # Return exit code
    return 0 if result.wasSuccessful() else 1

if __name__ == '__main__':
    sys.exit(main()) 