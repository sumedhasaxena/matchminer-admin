#!/bin/bash

# matchminer-admin File Watcher Startup Script
# This script starts the file watcher that processes JSON files every 2 hours

echo "========================================"
echo "    matchminer-admin File Watcher"
echo "========================================"
echo

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
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
python3 -c "import requests, loguru" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "ERROR: Required Python packages not found"
    echo "Please install dependencies: pip3 install -r requirements.txt"
    exit 1
fi

echo "All checks passed"
echo "Starting file watcher in background..."
echo "Logs will be written to watcher.log"
echo "To stop the watcher: pkill -f file_watcher.py"
echo "To view logs: tail -f watcher.log"
echo

# Start the file watcher in background with nohup (uses config.py interval)
nohup python3 file_watcher.py > watcher.log 2>&1 &

# Get the process ID
WATCHER_PID=$!
echo "File watcher started with PID: $WATCHER_PID"
echo "Background process is running. You can safely close this terminal."
echo
echo "Useful commands:"
echo "  Check if running: ps aux | grep file_watcher.py"
echo "  View logs: tail -f watcher.log"
echo "  Stop watcher: pkill -f file_watcher.py" 