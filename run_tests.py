#!/usr/bin/env python
"""Simple test runner script."""

import subprocess
import sys
import os


def run_tests():
    """Run all tests with coverage."""
    print("🧪 Running Data Pipeline Tests")
    print("=" * 50)
    
    # Change to project directory
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    
    # Run pytest with coverage
    try:
        cmd = [
            sys.executable, "-m", "pytest", 
            "tests/",
            "-v",
            "--tb=short",
            "--color=yes"
        ]
        
        result = subprocess.run(cmd, check=True)
        
        print("\n✅ All tests passed!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Tests failed with exit code {e.returncode}")
        return False


def run_specific_test(test_pattern):
    """Run specific test(s) matching a pattern."""
    print(f"🧪 Running tests matching: {test_pattern}")
    print("=" * 50)
    
    try:
        cmd = [
            sys.executable, "-m", "pytest",
            "tests/",
            "-v", 
            "-k", test_pattern,
            "--tb=short",
            "--color=yes"
        ]
        
        result = subprocess.run(cmd, check=True)
        print(f"\n✅ Tests matching '{test_pattern}' passed!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Tests failed with exit code {e.returncode}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Run specific test pattern
        pattern = sys.argv[1]
        success = run_specific_test(pattern)
    else:
        # Run all tests
        success = run_tests()
    
    sys.exit(0 if success else 1)