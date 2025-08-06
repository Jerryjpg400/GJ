#!/usr/bin/env python3
"""
Parallel CSV Processor - Multiprocessing Task Distributor
Handles parallel execution of csv_processor.py
"""

import os
import sys
import glob
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import subprocess
import time
import json


def process_single_file(csv_file):
    """Process a single CSV file using csv_processor.py."""
    try:
        start_time = time.time()
        
        # Call csv_processor.py as subprocess
        result = subprocess.run(
            [sys.executable, 'csv_processor.py', csv_file, '0'],
            capture_output=True,
            text=True
        )
        
        return {
            'file': csv_file,
            'success': result.returncode == 0,
            'time': time.time() - start_time,
            'error': result.stderr if result.returncode != 0 else None
        }
    except Exception as e:
        return {
            'file': csv_file,
            'success': False,
            'time': 0,
            'error': str(e)
        }


def find_csv_files(directory):
    """Find all CSV files in directory."""
    pattern = os.path.join(directory, '**', '*.csv')
    return glob.glob(pattern, recursive=True)


def load_checkpoint(checkpoint_file):
    """Load processed files from checkpoint."""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            return set(json.load(f).get('processed', []))
    return set()


def save_checkpoint(checkpoint_file, processed_files):
    """Save checkpoint with processed files."""
    with open(checkpoint_file, 'w') as f:
        json.dump({'processed': list(processed_files)}, f)


def process_files_parallel(directory, num_workers=None, checkpoint_file=None):
    """Process all CSV files in directory using parallel workers."""
    
    # Auto-detect workers if not specified
    if num_workers is None:
        num_workers = min(mp.cpu_count(), 16)
    
    # Find all CSV files
    csv_files = find_csv_files(directory)
    total_files = len(csv_files)
    
    if total_files == 0:
        print(f"No CSV files found in {directory}")
        return
    
    print(f"Found {total_files} CSV files")
    print(f"Using {num_workers} workers")
    
    # Load checkpoint
    processed = set()
    if checkpoint_file:
        processed = load_checkpoint(checkpoint_file)
        csv_files = [f for f in csv_files if f not in processed]
        print(f"Resuming from checkpoint: {len(processed)} already processed")
    
    # Process files in parallel
    completed = 0
    failed = 0
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(process_single_file, f): f for f in csv_files}
        
        # Process results as they complete
        for future in as_completed(future_to_file):
            result = future.result()
            completed += 1
            
            if result['success']:
                processed.add(result['file'])
            else:
                failed += 1
                print(f"\nFailed: {result['file']}: {result['error']}", file=sys.stderr)
            
            # Progress report
            if completed % 100 == 0 or completed == len(csv_files):
                elapsed = time.time() - start_time
                rate = completed / elapsed if elapsed > 0 else 0
                remaining = (len(csv_files) - completed) / rate if rate > 0 else 0
                
                print(f"\rProgress: {completed}/{len(csv_files)} "
                      f"({completed/len(csv_files)*100:.1f}%) "
                      f"Rate: {rate:.0f} files/sec "
                      f"ETA: {int(remaining//60)}:{int(remaining%60):02d} "
                      f"Failed: {failed}", end='', flush=True)
                
                # Save checkpoint periodically
                if checkpoint_file and completed % 1000 == 0:
                    save_checkpoint(checkpoint_file, processed)
    
    # Final report
    print(f"\n\nCompleted processing {total_files} files")
    print(f"Success: {total_files - failed}")
    print(f"Failed: {failed}")
    print(f"Total time: {time.time() - start_time:.1f} seconds")
    
    # Save final checkpoint
    if checkpoint_file:
        save_checkpoint(checkpoint_file, processed)


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: parallel_processor.py <directory> [num_workers] [checkpoint_file]")
        sys.exit(1)
    
    directory = sys.argv[1]
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else None
    checkpoint_file = sys.argv[3] if len(sys.argv) > 3 else None
    
    if not os.path.isdir(directory):
        print(f"Directory not found: {directory}", file=sys.stderr)
        sys.exit(1)
    
    process_files_parallel(directory, num_workers, checkpoint_file)


if __name__ == "__main__":
    main()