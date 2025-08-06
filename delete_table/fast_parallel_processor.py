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
from tqdm import tqdm


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
    failed = 0
    
    # Use process pool
    with mp.Pool(num_workers) as pool:
        # Map batches to workers
        batch_results = pool.map(partial(process_batch, column_index=0), batches)
        
        # Use tqdm to display progress
        with tqdm(total=total_files, desc="🚀 Processing CSV files", 
                  unit="files", ncols=100,
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            
            # Flatten results and report progress
            for batch_result in batch_results:
                for result in batch_result:
                    if not result['success']:
                        failed += 1
                        # Use tqdm.write to avoid interfering with progress bar
                        tqdm.write(f"❌ Failed: {result['file']}: {result['error']}")
                    
                    # Update progress bar
                    pbar.update(1)
                    
                    # Update real-time statistics
                    elapsed = time.time() - start_time
                    rate = pbar.n / elapsed if elapsed > 0 else 0
                    pbar.set_postfix({
                        'Success': pbar.n - failed,
                        'Failed': failed,
                        'Rate': f'{rate:.1f}/s'
                    })
    
    # Final summary
    elapsed = time.time() - start_time
    print(f"\n📊 Processing Summary:")
    print(f"⏱️  Completed in {elapsed:.1f} seconds")
    print(f"📁 Processed: {total_files} files")
    print(f"✅ Success: {total_files - failed}")
    print(f"❌ Failed: {failed}")
    print(f"🚀 Average rate: {total_files/elapsed:.0f} files/sec")


if __name__ == "__main__":
    main()