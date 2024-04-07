#!/bin/bash

# Stop on the first sign of trouble
set -e

# Print a message to indicate the start of the environment setup
echo "Starting environment setup..."

# Create a new Python virtual environment named etlenv in the current directory
echo "Creating a new virtual environment named etlenv with the default Python 3 version..."
python3 -m venv etlenv

# Activate the newly created environment
# Note: The method of activation depends on your shell. The below command works for bash/sh.
echo "Activating the etlenv environment..."
source etlenv/bin/activate

# Update pip to its latest version within the virtual environment
echo "Upgrading pip to the latest version..."
pip install --upgrade pip

# Install Python dependencies using pip from the requirements.txt in the current directory
echo "Installing Python dependencies from the requirements.txt..."
pip install -r ./requirements.txt

# Indicate the completion of the environment setup
echo "Environment setup is complete. The virtual environment 'etlenv' is now ready for use."
