#!/bin/bash

# matchminer-admin Data Processor Startup Script
# This script starts the data processor that processes patient and trial JSON files and sends to matchminer DB

echo "========================================"
echo "    matchminer-admin Data Processor"
echo "========================================"
echo

# Check if we're in the right directory
if [ ! -f "config.py" ]; then
    echo "ERROR: config.py not found"
    echo "Please run this script from the matchminer-admin directory"
    exit 1
fi

# Source the conda initialization and call the function
source ~/.init_conda
init_conda

# Activate the conda environment
ENV_NAME="matchminer_admin"

# Check if conda environment exists
if conda list -n "$ENV_NAME" >/dev/null 2>&1; then
    echo "Activating conda environment: $ENV_NAME"
    conda activate "$ENV_NAME"
else
    echo "Creating conda environment: $ENV_NAME"
    conda create -n "$ENV_NAME" python=3.12 -y
    
    if [ $? -ne 0 ]; then
        echo "ERROR: Failed to create conda environment '$ENV_NAME'"
        exit 1
    fi
    
    echo "Activating newly created conda environment: $ENV_NAME"
    conda activate "$ENV_NAME"
    
    echo "Installing requirements from requirements.txt..."
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
        if [ $? -ne 0 ]; then
            echo "ERROR: Failed to install requirements"
            exit 1
        fi
        echo "Requirements installed successfully"
    else
        echo "WARNING: requirements.txt not found, skipping package installation"
    fi
fi

echo "Starting data processor..."

# Set environment
export ENVIRONMENT=tobeset

echo "Processing files at $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

# Call the Python script to process files once
python data_processor.py
PROCESS_EXIT_CODE=$?

if [ $PROCESS_EXIT_CODE -eq 0 ]; then
    echo "Processing completed successfully"
else
    echo "Processing completed with errors (exit code: $PROCESS_EXIT_CODE)"
fi

echo "========================================"
echo "Data processor completed at $(date '+%Y-%m-%d %H:%M:%S')" 