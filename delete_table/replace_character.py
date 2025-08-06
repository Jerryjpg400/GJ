#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import shutil
import threading
import argparse
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Dict
from dataclasses import dataclass
from tqdm import tqdm

@dataclass
class ProcessResult:
    """Result of processing a single file"""
    file_path: str
    success: bool
    error_msg: str = ""
    original_name: str = ""
    new_name: str = ""

class ProcessStats:
    """Thread-safe statistics collection"""
    def __init__(self):
        self._lock = threading.Lock()
        self.total_files = 0
        self.success_count = 0
        self.error_count = 0
        self.renamed_count = 0
    
    def add_result(self, result: ProcessResult):
        with self._lock:
            if result.success:
                self.success_count += 1
                if result.new_name and result.new_name != result.original_name:
                    self.renamed_count += 1
            else:
                self.error_count += 1

def replace_character_in_csv_content(file_path, old_char='ԥ', new_char='豫'):
    """
    Replace character in CSV file content
    """
    temp_file = str(file_path) + '.tmp'
    
    try:
        with open(file_path, 'r', encoding='utf-8') as infile, \
             open(temp_file, 'w', encoding='utf-8', newline='') as outfile:
            
            content = infile.read()
            updated_content = content.replace(old_char, new_char)
            outfile.write(updated_content)
        
        # Replace original file with updated content
        shutil.move(temp_file, file_path)
        return True
        
    except Exception as e:
        # Clean up temp file if it exists
        if os.path.exists(temp_file):
            os.remove(temp_file)
        print(f"Error processing {file_path}: {e}")
        return False

def rename_file_with_character_replacement(file_path, old_char='ԥ', new_char='豫', verbose=False):
    """
    Rename file by replacing character in filename
    """
    try:
        file_path = Path(file_path)
        old_name = file_path.name
        
        if old_char in old_name:
            new_name = old_name.replace(old_char, new_char)
            new_path = file_path.parent / new_name
            file_path.rename(new_path)
            if verbose:
                print(f"Renamed: {old_name} -> {new_name}")
            return str(new_path)
        
        return str(file_path)
        
    except Exception as e:
        print(f"Error renaming {file_path}: {e}")
        return str(file_path)

def process_single_file(file_path: Path, old_char: str = 'ԥ', new_char: str = '豫', verbose: bool = False) -> ProcessResult:
    """
    Process a single file: replace content and rename if needed
    """
    result = ProcessResult(
        file_path=str(file_path),
        success=False,
        original_name=file_path.name
    )
    
    try:
        # Step 1: Replace content if it's a CSV file
        if file_path.suffix.lower() == '.csv':
            if not replace_character_in_csv_content(file_path, old_char, new_char):
                result.error_msg = "Failed to replace CSV content"
                return result
        
        # Step 2: Rename file if needed
        if old_char in file_path.name:
            new_path = rename_file_with_character_replacement(file_path, old_char, new_char, verbose)
            result.new_name = Path(new_path).name
        else:
            result.new_name = file_path.name
        
        result.success = True
        return result
        
    except Exception as e:
        result.error_msg = str(e)
        return result

def process_files_parallel(files: List[Path], old_char: str = 'ԥ', new_char: str = '豫', max_workers: int = None, verbose: bool = False) -> ProcessStats:
    """
    Process files in parallel using ThreadPoolExecutor
    """
    if not files:
        return ProcessStats()
    
    stats = ProcessStats()
    stats.total_files = len(files)
    
    # Use optimal number of workers for I/O bound tasks
    if max_workers is None:
        max_workers = min(32, (os.cpu_count() or 1) + 4)
    
    print(f"🚀 Processing {len(files)} files with {max_workers} workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_single_file, file_path, old_char, new_char, verbose): file_path 
            for file_path in files
        }
        
        # Use tqdm to display progress
        with tqdm(total=len(files), desc="⚙️ Processing files", 
                  unit="files", ncols=100,
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            
            # Process completed tasks
            for future in as_completed(future_to_file):
                result = future.result()
                stats.add_result(result)
                
                # Update progress bar
                pbar.update(1)
                
                # Update real-time statistics
                pbar.set_postfix({
                    'Success': stats.success_count,
                    'Errors': stats.error_count,
                    'Renamed': stats.renamed_count
                })
                
                # Use tqdm.write for error and rename messages to avoid interfering with progress bar
                if not result.success:
                    tqdm.write(f"❌ ERROR processing {result.file_path}: {result.error_msg}")
                elif verbose and result.new_name != result.original_name:
                    tqdm.write(f"📝 Renamed: {result.original_name} → {result.new_name}")
    
    return stats

def collect_files_to_process(processed_dir: Path, old_char: str) -> List[Path]:
    """
    Collect all files that need processing
    """
    files_to_process = []
    
    if processed_dir.exists():
        print(f"🔍 Scanning directory: {processed_dir}")
        
        # First pass to count total files for progress bar
        all_paths = list(processed_dir.rglob("*"))
        
        with tqdm(all_paths, desc="📁 Scanning files", unit="files", ncols=100) as pbar:
            for file_path in pbar:
                if file_path.is_file() and old_char in file_path.name:
                    files_to_process.append(file_path)
                    pbar.set_postfix({'Found': len(files_to_process)})
        
        print(f"✅ Found {len(files_to_process)} files that need processing")
    
    return files_to_process

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Character replacement tool for files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Directory Naming Logic:
  The processed directory is automatically named based on the input directory.
  Format: {INPUT_FOLDER_NAME}_处理后
  
  Examples:
    Input:  /PATH/To/20231201
    Output: /PATH/To/20231201_处理后
    
    Input:  /home/user/data_batch_01
    Output: /home/user/data_batch_01_处理后

Usage Examples:
  python replace_character.py -i /path/to/20231201
  python replace_character.py -i /path/to/folder -o 'ԥ' -n '豫' -w 8
  python replace_character.py -i /path/to/data --dry-run
  python replace_character.py --help
        """
    )
    
    parser.add_argument('-i', '--input', type=str, required=True,
                       help='Input directory path to process files')
    parser.add_argument('-o', '--old-char', type=str, default='ԥ',
                       help='Character to replace (default: ԥ)')
    parser.add_argument('-n', '--new-char', type=str, default='豫',
                       help='Replacement character (default: 豫)')
    parser.add_argument('-w', '--workers', type=int, default=None,
                       help='Number of worker threads (default: auto-detect)')
    parser.add_argument('-t', '--target-csv', type=str, default='ԥN00775D.csv',
                       help='Specific CSV file to process (default: ԥN00775D.csv)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without making changes')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Show detailed processing information')
    
    return parser.parse_args()

def generate_processed_dir_name(input_dir: Path) -> Path:
    """
    Generate processed directory name based on input directory name
    Format: {INPUT_FOLDER_NAME}_处理后
    """
    input_folder_name = input_dir.name
    processed_folder_name = f"{input_folder_name}_处理后"
    processed_dir = input_dir.parent / processed_folder_name
    return processed_dir

def main():
    start_time = time.time()
    args = parse_arguments()
    
    # Parse arguments
    base_dir = Path(args.input).resolve()
    target_csv = base_dir / args.target_csv
    processed_dir = generate_processed_dir_name(base_dir)
    old_char = args.old_char
    new_char = args.new_char
    max_workers = args.workers
    dry_run = args.dry_run
    verbose = args.verbose
    
    print("=== Character Replacement Script ===")
    print(f"Input directory: {base_dir}")
    print(f"Processed directory: {processed_dir}")
    print(f"Replacing '{old_char}' with '{new_char}'")
    if dry_run:
        print("*** DRY RUN MODE - No changes will be made ***")
    print()
    
    # Validate input directory
    if not base_dir.exists():
        print(f"Error: Input directory does not exist: {base_dir}")
        return 1
    
    if not base_dir.is_dir():
        print(f"Error: Input path is not a directory: {base_dir}")
        return 1
    
    # Process the specific CSV file if it exists in root directory
    if target_csv.exists():
        print(f"Processing CSV content: {target_csv}")
        if dry_run:
            print("  (DRY RUN) Would replace CSV content and rename file")
        else:
            if replace_character_in_csv_content(target_csv, old_char, new_char):
                print("✓ CSV content updated successfully")
                
                # Rename the file
                new_path = rename_file_with_character_replacement(target_csv, old_char, new_char, verbose)
                if verbose:
                    print(f"✓ File renamed to: {Path(new_path).name}")
            else:
                print("✗ Failed to update CSV content")
    else:
        # Check if it exists in processed directory
        target_csv_in_processed = processed_dir / args.target_csv
        if target_csv_in_processed.exists():
            print(f"Processing CSV content: {target_csv_in_processed}")
            if dry_run:
                print("  (DRY RUN) Would replace CSV content and rename file")
            else:
                if replace_character_in_csv_content(target_csv_in_processed, old_char, new_char):
                    print("✓ CSV content updated successfully")
                    
                    # Rename the file
                    new_path = rename_file_with_character_replacement(target_csv_in_processed, old_char, new_char, verbose)
                    if verbose:
                        print(f"✓ File renamed to: {Path(new_path).name}")
                else:
                    print("✗ Failed to update CSV content")
    
    print()
    
    # Batch process all files in the processed directory
    if processed_dir.exists():
        print(f"Processing files in directory: {processed_dir}")
        
        # Collect all files that need processing
        files_to_process = collect_files_to_process(processed_dir, old_char)
        
        print(f"Found {len(files_to_process)} files to process")
        
        if files_to_process:
            if dry_run:
                print("=== DRY RUN: Files that would be processed ===")
                for file_path in files_to_process:
                    old_name = file_path.name
                    if old_char in old_name:
                        new_name = old_name.replace(old_char, new_char)
                        print(f"  Would rename: {old_name} -> {new_name}")
                    else:
                        print(f"  Would process: {old_name}")
                print(f"Total files that would be processed: {len(files_to_process)}")
            else:
                # Process files in parallel
                stats = process_files_parallel(files_to_process, old_char, new_char, max_workers, verbose)
                
                # Print summary
                print(f"\n📊 === Processing Summary ===")
                print(f"📁 Total files: {stats.total_files}")
                print(f"✅ Successfully processed: {stats.success_count}")
                print(f"📝 Files renamed: {stats.renamed_count}")
                if stats.error_count > 0:
                    print(f"❌ Errors: {stats.error_count}")
        else:
            print("No files found with the target character")
    else:
        print(f"Warning: Processed directory not found: {processed_dir}")
        
        # Try to create processed directory if input directory has files to copy
        print("Looking for files in input directory to copy to processed directory...")
        input_files = list(base_dir.glob("*"))
        csv_files = [f for f in input_files if f.is_file() and f.suffix.lower() == '.csv']
        
        if csv_files and not dry_run:
            print(f"Creating processed directory: {processed_dir}")
            processed_dir.mkdir(parents=True, exist_ok=True)
            
            # Copy CSV files to processed directory
            for csv_file in csv_files:
                dest_file = processed_dir / csv_file.name
                shutil.copy2(csv_file, dest_file)
                if verbose:
                    print(f"Copied: {csv_file.name} -> {dest_file}")
            
            # Now process the copied files
            files_to_process = collect_files_to_process(processed_dir, old_char)
            if files_to_process:
                print(f"⚙️ Processing {len(files_to_process)} copied files...")
                stats = process_files_parallel(files_to_process, old_char, new_char, max_workers, verbose)
                
                print(f"\n📊 === Processing Summary ===")
                print(f"📁 Total files: {stats.total_files}")
                print(f"✅ Successfully processed: {stats.success_count}")
                print(f"📝 Files renamed: {stats.renamed_count}")
                if stats.error_count > 0:
                    print(f"❌ Errors: {stats.error_count}")
        elif csv_files and dry_run:
            print(f"(DRY RUN) Would create directory: {processed_dir}")
            print("(DRY RUN) Would copy the following CSV files:")
            for csv_file in csv_files:
                print(f"  Would copy: {csv_file.name}")
        else:
            print("No CSV files found in input directory to process")
    
    end_time = time.time()
    total_time = end_time - start_time
    print(f"\n✅ === Script completed in {total_time:.2f} seconds ===")
    return 0

if __name__ == "__main__":
    exit(main())