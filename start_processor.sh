#!/bin/bash

# matchminer-admin Data Processor Startup Script
# This script starts the data processor that processes JSON files on a schedule

echo "========================================"
echo "    matchminer-admin Data Processor"
echo "========================================"
echo

# Check if Python 3 is available
if ! command -v python &> /dev/null; then
    echo "ERROR: Python 3 is not installed or not in PATH"
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

echo "All checks passed"
echo "Starting data processor in background..."
echo "Logs will be written to processor.log"
echo "To stop the processor: pkill -f data_processor.py"
echo "To view logs: tail -f processor.log"
echo

# Start the data processor in background with nohup (uses config.py interval)
nohup python data_processor.py > processor.log 2>&1 &

# Get the process ID
PROCESSOR_PID=$!
echo "Data processor started with PID: $PROCESSOR_PID"
echo "Background process is running. You can safely close this terminal."
echo
echo "Useful commands:"
echo "  Check if running: ps aux | grep data_processor.py"
echo "  View logs: tail -f processor.log"
echo "  Stop processor: pkill -f data_processor.py" 