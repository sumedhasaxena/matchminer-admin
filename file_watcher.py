#!/usr/bin/env python3
"""
Simple file watcher for Matchminer.
Processes files immediately when they're added to the directories.
"""

import os
import sys
import time
import signal
import argparse
from datetime import datetime
from pathlib import Path
from loguru import logger

# Import the processing functions
try:
    from patient import insert_all_patient_documents
    from trial import insert_trials
    import config
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

class SimpleFileWatcher:
    def __init__(self):
        self.running = True
        self.processed_files = set()
        self.stats = {
            "files_detected": 0,
            "files_processed": 0,
            "processing_runs": 0,
            "last_activity": None
        }
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Get directories to watch
        self.watch_dirs = {
            "patient_clinical": os.path.join(config.PATIENT_FOLDER, config.PATIENT_CLINICAL_JSON_FOLDER),
            "patient_genomic": os.path.join(config.PATIENT_FOLDER, config.PATIENT_GENOMIC_JSON_FOLDER),
            "trial": config.TRIAL_FOLDER
        }
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\nReceived signal {signum}, shutting down gracefully...")
        self.running = False
    
    def _get_all_json_files(self):
        """Get all JSON files in watched directories."""
        all_files = set()
        
        for dir_name, dir_path in self.watch_dirs.items():
            if os.path.exists(dir_path):
                for file in os.listdir(dir_path):
                    if file.endswith('.json'):
                        full_path = os.path.join(dir_path, file)
                        if os.path.isfile(full_path):
                            all_files.add(full_path)
        
        return all_files
    
    def _has_new_files(self):
        """Check if there are new files to process."""
        current_files = self._get_all_json_files()
        new_files = current_files - self.processed_files
        
        if new_files:
            print(f"New files detected: {len(new_files)}")
            for file in new_files:
                print(f"  + {os.path.basename(file)}")
            return True
        
        return False
    
    def process_files(self):
        """Process all files and update tracking."""
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
        
        # Update tracking
        if success:
            current_files = self._get_all_json_files()
            self.processed_files.update(current_files)
            self.stats["files_processed"] = len(self.processed_files)
            self.stats["processing_runs"] += 1
            self.stats["last_activity"] = datetime.now()
        
        return success
    
    def run(self, check_interval=7200):
        """Main watching loop."""
        hours = check_interval // 3600
        minutes = (check_interval % 3600) // 60
        
        print("Simple File Watcher Started")
        print(f"Checking every {hours} hours {minutes} minutes ({check_interval} seconds)")
        print("Press Ctrl+C to stop")
        print("=" * 50)
        
        # Initial scan
        initial_files = self._get_all_json_files()
        self.processed_files.update(initial_files)
        print(f"Initial scan: {len(initial_files)} existing files")
        
        while self.running:
            try:
                # Check for new files
                if self._has_new_files():
                    self.stats["files_detected"] += 1
                    success = self.process_files()
                    
                    if success:
                        print(f"Processing completed successfully")
                    else:
                        print(f"Processing completed with errors")
                else:
                    # Show status every hour for long intervals
                    if self.stats["last_activity"]:
                        time_since = (datetime.now() - self.stats["last_activity"]).total_seconds() / 3600
                        if time_since > 1:
                            print(f"No new files. Last activity: {time_since:.1f} hours ago")
                            self.stats["last_activity"] = datetime.now()
                
                # Sleep
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
        
        # Final stats
        print("\n" + "=" * 50)
        print("Final Statistics:")
        print(f"  Files detected: {self.stats['files_detected']}")
        print(f"  Files processed: {self.stats['files_processed']}")
        print(f"  Processing runs: {self.stats['processing_runs']}")
        print(f"  Last activity: {self.stats['last_activity']}")
        print("File watcher stopped")

def main():
    parser = argparse.ArgumentParser(description="Simple file watcher for Matchminer")
    parser.add_argument("--minutes", type=int, default=None,
                       help="Check interval in minutes (overrides config)")
    parser.add_argument("--once", action="store_true",
                       help="Process files once and exit")
    
    args = parser.parse_args()
    
    # Determine interval: command line args override config
    if args.minutes is not None:
        interval_seconds = args.minutes * 60
    else:
        # Use config default
        interval_seconds = config.WATCHER_INTERVAL_MINUTES * 60
    
    watcher = SimpleFileWatcher()
    
    if args.once:
        print("Processing files once...")
        success = watcher.process_files()
        sys.exit(0 if success else 1)
    else:
        total_minutes = interval_seconds // 60
        print(f"File watcher will check every {total_minutes} minutes")
        watcher.run(interval_seconds)

if __name__ == "__main__":
    main() 