"""Setup script for the data pipeline project."""

import subprocess
import sys
import os
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors."""
    print(f"\n{description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {description} completed")
        if result.stdout:
            print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed: {e}")
        if e.stderr:
            print(f"Error: {e.stderr}")
        return False
    return True


def check_python_version():
    """Check if Python version is compatible."""
    if sys.version_info < (3, 8):
        print("✗ Python 3.8 or higher is required")
        return False
    print(f"✓ Python {sys.version.split()[0]} detected")
    return True


def create_virtual_environment():
    """Create a virtual environment."""
    if os.path.exists("venv"):
        print("✓ Virtual environment already exists")
        return True
    
    return run_command("python -m venv venv", "Creating virtual environment")


def install_dependencies():
    """Install project dependencies."""
    # Determine the correct pip path
    pip_cmd = "venv/bin/pip" if os.name != 'nt' else "venv\\Scripts\\pip"
    
    if not os.path.exists(pip_cmd.split('/')[0]):
        print("Virtual environment not found. Please create it first.")
        return False
    
    return run_command(f"{pip_cmd} install -r requirements.txt", "Installing dependencies")


def run_tests():
    """Run the test suite."""
    python_cmd = "venv/bin/python" if os.name != 'nt' else "venv\\Scripts\\python"
    return run_command(f"{python_cmd} -m pytest tests/ -v", "Running tests")


def setup_environment_file():
    """Set up the environment file."""
    if os.path.exists(".env"):
        print("✓ .env file already exists")
        return True
    
    try:
        # Copy .env.example to .env
        with open(".env.example", "r") as src, open(".env", "w") as dst:
            dst.write(src.read())
        print("✓ Created .env file from .env.example")
        print("  Please edit .env file with your actual configuration values")
        return True
    except Exception as e:
        print(f"✗ Failed to create .env file: {e}")
        return False


def main():
    """Main setup function."""
    print("Data Pipeline POC Setup")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Create virtual environment
    if not create_virtual_environment():
        print("Failed to create virtual environment")
        sys.exit(1)
    
    # Install dependencies
    if not install_dependencies():
        print("Failed to install dependencies")
        sys.exit(1)
    
    # Set up environment file
    if not setup_environment_file():
        print("Failed to set up environment file")
        sys.exit(1)
    
    # Run tests
    print("\nRunning tests to verify installation...")
    if run_tests():
        print("\n✓ Setup completed successfully!")
        print("\nNext steps:")
        print("1. Edit .env file with your Kafka and MongoDB credentials")
        print("2. Start the test data generator: python test_data_generator/generator.py")
        print("3. Start the pipeline: python pipeline/main.py")
    else:
        print("\n✗ Some tests failed. Please check the installation.")
        sys.exit(1)


if __name__ == "__main__":
    main()