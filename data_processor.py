#!/usr/bin/env python3
"""
Data Processor for Matchminer's trial data files and patient data files.
"""

import os
import sys
from datetime import datetime

# Import the processing functions
try:
    from patient import insert_all_patient_documents
    from trial import process_trials
    import config
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

class DataProcessor:
    def __init__(self):
        pass
    
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
            trial_success = process_trials()
            if trial_success:
                print("  Trial files processed successfully")
            else:
                print("  No trial files to process or processing failed")
        except Exception as e:
            print(f"  Trial files failed: {e}")
            success = False
        
        return success

def main():
    """Main entry point - processes files once and exits."""
    processor = DataProcessor()
    success = processor.process_files()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 