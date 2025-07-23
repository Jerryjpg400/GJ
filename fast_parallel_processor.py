#!/usr/bin/env python3
"""
Fast Parallel CSV Processor - Optimized version
Direct multiprocessing without subprocess overhead
"""

import os
import sys
import csv
import time
import json
import tempfile
import shutil
import multiprocessing as mp
from pathlib import Path
from functools import partial


def remove_column_direct(csv_file, column_index=0):
    """Remove column from CSV file - optimized version."""
    temp_fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(csv_file))
    
    try:
        with os.fdopen(temp_fd, 'w', newline='', encoding='utf-8') as temp_file:
            with open(csv_file, 'r', newline='', encoding='utf-8-sig') as infile:
                reader = csv.reader(infile)
                writer = csv.writer(temp_file)
                
                for row in reader:
                    if len(row) > column_index:
                        row.pop(column_index)
                    writer.writerow(row)
        
        shutil.move(temp_path, csv_file)
        return True, None
        
    except Exception as e:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False, str(e)


def process_file_worker(csv_file, column_index=0):
    """Worker function for processing single file."""
    start_time = time.time()
    success, error = remove_column_direct(csv_file, column_index)
    
    return {
        'file': csv_file,
        'success': success,
        'time': time.time() - start_time,
        'error': error
    }


def process_batch(files_batch, column_index=0):
    """Process a batch of files - for better load balancing."""
    results = []
    for csv_file in files_batch:
        results.append(process_file_worker(csv_file, column_index))
    return results


def create_batches(files, num_batches):
    """Create balanced batches of files."""
    batch_size = len(files) // num_batches + 1
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]


def main():
    """Main function with optimized parallel processing."""
    if len(sys.argv) < 2:
        print("Usage: fast_parallel_processor.py <directory> [num_workers]")
        sys.exit(1)
    
    directory = sys.argv[1]
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else mp.cpu_count()
    
    # Find all CSV files
    csv_files = list(Path(directory).rglob('*.csv'))
    total_files = len(csv_files)
    
    if total_files == 0:
        print(f"No CSV files found in {directory}")
        return
    
    print(f"Processing {total_files} CSV files with {num_workers} workers")
    
    # Create batches for better load balancing
    file_paths = [str(f) for f in csv_files]
    batches = create_batches(file_paths, num_workers * 4)  # More batches than workers
    
    # Process in parallel
    start_time = time.time()
    completed = 0
    failed = 0
    
    # Use process pool
    with mp.Pool(num_workers) as pool:
        # Map batches to workers
        batch_results = pool.map(partial(process_batch, column_index=0), batches)
        
        # Flatten results and report progress
        for batch_result in batch_results:
            for result in batch_result:
                completed += 1
                if not result['success']:
                    failed += 1
                    print(f"Failed: {result['file']}: {result['error']}")
                
                # Progress update
                if completed % 100 == 0 or completed == total_files:
                    elapsed = time.time() - start_time
                    rate = completed / elapsed
                    percent = completed / total_files * 100
                    
                    print(f"Progress: {completed}/{total_files} ({percent:.1f}%) "
                          f"Rate: {rate:.0f} files/sec", end='\r')
    
    # Final summary
    elapsed = time.time() - start_time
    print(f"\n\nCompleted in {elapsed:.1f} seconds")
    print(f"Processed: {total_files} files")
    print(f"Success: {total_files - failed}")
    print(f"Failed: {failed}")
    print(f"Average rate: {total_files/elapsed:.0f} files/sec")


if __name__ == "__main__":
    main()