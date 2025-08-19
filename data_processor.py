#!/usr/bin/env python3
"""
Data Processor for Matchminer's trial data files and patient data files.
Processes them at configured intervals or just once, based on supplied command line arguments
"""

import os
import sys
import time
import signal
import argparse
from datetime import datetime

# Import the processing functions
try:
    from patient import insert_all_patient_documents
    from trial import insert_trials
    import config
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

class DataProcessor:
    def __init__(self):
        self.running = True
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.running = False
    
    def process_files(self):
        """Process all files."""
        print(f"\nProcessing files at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        success = True
        
        # Process patient files
        try:
            print("  Processing patient files...")
            patient_success = insert_all_patient_documents()
            if patient_success:
                print("  Patient files processed successfully")
            else:
                print("  No patient files to process or processing failed")
        except Exception as e:
            print(f"  Patient files failed: {e}")
            success = False
        
        # Process trial files
        try:
            print("  Processing trial files...")
            trial_success = insert_trials()
            if trial_success:
                print("  Trial files processed successfully")
            else:
                print("  No trial files to process or processing failed")
        except Exception as e:
            print(f"  Trial files failed: {e}")
            success = False
        
        return success
    
    def run(self, check_interval=7200):
        """Main loop that runs on configured interval."""
        hours = check_interval // 3600
        minutes = (check_interval % 3600) // 60
        
        print("Data Processor Started")
        print(f"Running every {hours} hours {minutes} minutes ({check_interval} seconds)")
        print("Press Ctrl+C to stop")
        print("=" * 50)
        
        while self.running:
            try:
                # Process files first
                success = self.process_files()
                if success:
                    print("Processing completed successfully")
                else:
                    print("Processing completed with errors")
                
                # Then sleep for the configured interval
                for _ in range(check_interval):
                    if not self.running:
                        break
                    time.sleep(1)
                
            except KeyboardInterrupt:
                print("\nStopped by user")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                time.sleep(60)  # Wait 1 minute before retrying
        
        print("\n" + "=" * 50)
        print("Data processor stopped")

def main():
    parser = argparse.ArgumentParser(description="Simple data processor for Matchminer")
    parser.add_argument("--minutes", type=int, default=None,
                       help="Check interval in minutes (overrides config)")
    parser.add_argument("--once", action="store_true",
                       help="Process files once and exit")
    
    args = parser.parse_args()
        
    if args.minutes is not None:
        interval_seconds = args.minutes * 60
    else:
        # Use config default
        interval_seconds = config.WATCHER_INTERVAL_MINUTES * 60
    
    processor = DataProcessor()
    
    if args.once:
        print("Processing files once...")
        success = processor.process_files()
        sys.exit(0 if success else 1)
    else:
        total_minutes = interval_seconds // 60
        print(f"Data processor will run every {total_minutes} minutes")
        processor.run(interval_seconds)

if __name__ == "__main__":
    main() 