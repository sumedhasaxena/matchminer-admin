#!/bin/bash

# matchminer-admin Data Processor Startup Script
# This script starts the data processor that processes patient and trial JSON files and sends to matchminer DB

echo "========================================"
echo "    matchminer-admin Data Processor"
echo "========================================"
echo

# Check if Python is available
if ! command -v python &> /dev/null; then
    echo "ERROR: Python is not installed or not in PATH"
    echo "Please install Python 3 and try again"
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "config.py" ]; then
    echo "ERROR: config.py not found"
    echo "Please run this script from the matchminer-admin directory"
    exit 1
fi

# Check if required Python packages are installed
echo "Checking dependencies..."
python -c "import requests, loguru" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: Required Python packages not found"
    echo "Please install dependencies: pip3 install -r requirements.txt"
    exit 1
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