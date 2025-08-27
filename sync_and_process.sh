#!/bin/bash

# Master script that runs sync_trials.sh first, then start_processor.sh
# Both run once and exit (no loops) - designed for systemd timer

echo "========================================"
echo "    Matchminer Daily Tasks"
echo "========================================"
echo "Starting at $(date '+%Y-%m-%d %H:%M:%S')"
echo

# Set environment
export ENVIRONMENT=production #when  set to 'production', config.py loads .env. When set to 'development', it loads .env.dev

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NCT2CTML_DIR="$(dirname "$SCRIPT_DIR")/nct2ctml"

# Check if nct2ctml directory exists
if [ ! -d "$NCT2CTML_DIR" ]; then
    echo "ERROR: nct2ctml directory not found at: $NCT2CTML_DIR"
    echo "Please ensure nct2ctml is in the same parent directory as matchminer-admin"
    exit 1
fi

# Step 1: Sync trials
echo "Step 1: Syncing trials from clinicaltrials.gov..."
echo "Working directory: $NCT2CTML_DIR"
cd "$NCT2CTML_DIR"

# Check if sync_trials.sh exists
if [ ! -f "sync_trials.sh" ]; then
    echo "ERROR: sync_trials.sh not found in $NCT2CTML_DIR"
    exit 1
fi

# Make sure script is executable
chmod +x sync_trials.sh

# Run sync_trials.sh
./sync_trials.sh
SYNC_EXIT_CODE=$?

if [ $SYNC_EXIT_CODE -eq 0 ]; then
    echo "Trial sync completed successfully"
else
    echo "ERROR: Trial sync failed with exit code $SYNC_EXIT_CODE"
    echo "Aborting - will not proceed to file processing"
    exit $SYNC_EXIT_CODE
fi

echo
echo "----------------------------------------"

# Step 2: Process files
echo "Step 2: Processing patient and trial files..."
echo "Working directory: $SCRIPT_DIR"
cd "$SCRIPT_DIR"

# Check if start_processor.sh exists
if [ ! -f "start_processor.sh" ]; then
    echo "ERROR: start_processor.sh not found in $SCRIPT_DIR"
    exit 1
fi

# Make sure script is executable
chmod +x start_processor.sh

# Run start_processor.sh
./start_processor.sh
PROCESS_EXIT_CODE=$?

if [ $PROCESS_EXIT_CODE -eq 0 ]; then
    echo "File processing completed successfully"
else
    echo "ERROR: File processing failed with exit code $PROCESS_EXIT_CODE"
    exit $PROCESS_EXIT_CODE
fi

echo
echo "========================================"
echo "All daily tasks completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"
