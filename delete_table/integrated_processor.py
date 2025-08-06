#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Integrated CSV Processor
Combines fast parallel CSV column removal with character replacement and file renaming
"""

import os
import csv
import shutil
import threading
import argparse
import time
import tempfile
import multiprocessing as mp
from pathlib import Path
from functools import partial
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional
from dataclasses import dataclass
from tqdm import tqdm


@dataclass
class ProcessingStats:
    """Statistics for the entire processing pipeline"""
    total_files: int = 0
    column_removal_success: int = 0
    column_removal_failed: int = 0
    character_replacement_success: int = 0
    character_replacement_failed: int = 0
    files_renamed: int = 0
    processing_time: float = 0.0


def remove_column_from_csv(csv_file: str, column_index: int = 0) -> tuple[bool, Optional[str]]:
    """Remove specified column from CSV file"""
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


def process_csv_batch(files_batch: List[str], column_index: int = 0) -> List[Dict]:
    """Process a batch of CSV files for column removal"""
    results = []
    for csv_file in files_batch:
        start_time = time.time()
        success, error = remove_column_from_csv(csv_file, column_index)
        
        results.append({
            'file': csv_file,
            'success': success,
            'time': time.time() - start_time,
            'error': error
        })
    return results


def create_batches(files: List[str], num_batches: int) -> List[List[str]]:
    """Create balanced batches of files"""
    batch_size = len(files) // num_batches + 1
    return [files[i:i + batch_size] for i in range(0, len(files), batch_size)]


def replace_character_in_csv_content(file_path: Path, old_char: str = 'ԥ', new_char: str = '豫') -> bool:
    """Replace character in CSV file content"""
    temp_file = str(file_path) + '.tmp'
    
    try:
        with open(file_path, 'r', encoding='utf-8') as infile, \
             open(temp_file, 'w', encoding='utf-8', newline='') as outfile:
            
            content = infile.read()
            updated_content = content.replace(old_char, new_char)
            outfile.write(updated_content)
        
        shutil.move(temp_file, file_path)
        return True
        
    except Exception as e:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return False


def rename_file_with_character_replacement(file_path: Path, old_char: str = 'ԥ', new_char: str = '豫') -> tuple[str, bool]:
    """Rename file by replacing character in filename"""
    try:
        old_name = file_path.name
        
        if old_char in old_name:
            new_name = old_name.replace(old_char, new_char)
            new_path = file_path.parent / new_name
            file_path.rename(new_path)
            return str(new_path), True
        
        return str(file_path), False
        
    except Exception as e:
        return str(file_path), False


def process_single_file_character_replacement(file_path: Path, old_char: str = 'ԥ', new_char: str = '豫', verbose: bool = False) -> Dict:
    """Process a single file for character replacement and renaming"""
    result = {
        'file_path': str(file_path),
        'success': False,
        'error_msg': '',
        'original_name': file_path.name,
        'new_name': file_path.name,
        'renamed': False
    }
    
    try:
        # Step 1: Replace content if it's a CSV file
        if file_path.suffix.lower() == '.csv':
            if not replace_character_in_csv_content(file_path, old_char, new_char):
                result['error_msg'] = "Failed to replace CSV content"
                return result
        
        # Step 2: Rename file if needed
        if old_char in file_path.name:
            new_path, renamed = rename_file_with_character_replacement(file_path, old_char, new_char)
            result['new_name'] = Path(new_path).name
            result['renamed'] = renamed
            if verbose and renamed:
                print(f"📝 Renamed: {result['original_name']} → {result['new_name']}")
        
        result['success'] = True
        return result
        
    except Exception as e:
        result['error_msg'] = str(e)
        return result


def phase1_column_removal(directory: Path, column_index: int = 0, num_workers: int = None, verbose: bool = False) -> Dict:
    """Phase 1: Remove specified column from all CSV files"""
    print(f"🔧 === Phase 1: Column Removal ===")
    
    # Find all CSV files
    csv_files = list(directory.rglob('*.csv'))
    total_files = len(csv_files)
    
    if total_files == 0:
        print(f"No CSV files found in {directory}")
        return {'success': 0, 'failed': 0, 'total': 0}
    
    if num_workers is None:
        num_workers = mp.cpu_count()
    
    print(f"Processing {total_files} CSV files with {num_workers} workers")
    
    # Create batches for better load balancing
    file_paths = [str(f) for f in csv_files]
    batches = create_batches(file_paths, num_workers * 4)
    
    # Process in parallel
    start_time = time.time()
    failed = 0
    
    with mp.Pool(num_workers) as pool:
        batch_results = pool.map(partial(process_csv_batch, column_index=column_index), batches)
        
        with tqdm(total=total_files, desc="🗑️  Removing columns", 
                  unit="files", ncols=100,
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            
            for batch_result in batch_results:
                for result in batch_result:
                    if not result['success']:
                        failed += 1
                        if verbose:
                            tqdm.write(f"❌ Failed: {result['file']}: {result['error']}")
                    
                    pbar.update(1)
                    pbar.set_postfix({
                        'Success': pbar.n - failed,
                        'Failed': failed
                    })
    
    elapsed = time.time() - start_time
    success = total_files - failed
    
    print(f"✅ Phase 1 completed in {elapsed:.1f}s - Success: {success}, Failed: {failed}")
    
    return {'success': success, 'failed': failed, 'total': total_files, 'time': elapsed}


def phase2_character_replacement(directory: Path, old_char: str = 'ԥ', new_char: str = '豫', 
                                max_workers: int = None, verbose: bool = False) -> Dict:
    """Phase 2: Replace characters in content and filenames"""
    print(f"🔤 === Phase 2: Character Replacement ===")
    
    # Find all files that need processing
    all_files = list(directory.rglob("*"))
    files_to_process = [f for f in all_files if f.is_file() and (old_char in f.name or f.suffix.lower() == '.csv')]
    
    if not files_to_process:
        print("No files found that need character replacement")
        return {'success': 0, 'failed': 0, 'renamed': 0, 'total': 0}
    
    if max_workers is None:
        max_workers = min(32, (os.cpu_count() or 1) + 4)
    
    print(f"Processing {len(files_to_process)} files with {max_workers} workers")
    
    success_count = 0
    error_count = 0
    renamed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_single_file_character_replacement, file_path, old_char, new_char, verbose): file_path 
            for file_path in files_to_process
        }
        
        with tqdm(total=len(files_to_process), desc="🔤 Replacing characters", 
                  unit="files", ncols=100,
                  bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            
            for future in as_completed(future_to_file):
                result = future.result()
                
                if result['success']:
                    success_count += 1
                    if result['renamed']:
                        renamed_count += 1
                else:
                    error_count += 1
                    if verbose:
                        tqdm.write(f"❌ ERROR processing {result['file_path']}: {result['error_msg']}")
                
                pbar.update(1)
                pbar.set_postfix({
                    'Success': success_count,
                    'Errors': error_count,
                    'Renamed': renamed_count
                })
    
    print(f"✅ Phase 2 completed - Success: {success_count}, Errors: {error_count}, Renamed: {renamed_count}")
    
    return {
        'success': success_count, 
        'failed': error_count, 
        'renamed': renamed_count, 
        'total': len(files_to_process)
    }


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Integrated CSV Processor - Column removal + Character replacement",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Processing Pipeline:
  1. Phase 1: Remove specified column from all CSV files (multiprocessing)
  2. Phase 2: Replace characters in file content and rename files (threading)

Directory Structure:
  Input:  /path/to/input_folder
  Output: /path/to/input_folder_处理后 (auto-created if needed)

Usage Examples:
  python integrated_processor.py -i /path/to/data
  python integrated_processor.py -i /path/to/data -c 1 -o 'ԥ' -n '豫' --mp-workers 8 --thread-workers 16
  python integrated_processor.py -i /path/to/data --skip-column-removal
  python integrated_processor.py -i /path/to/data --skip-character-replacement
  python integrated_processor.py -i /path/to/data --dry-run -v
        """
    )
    
    parser.add_argument('-i', '--input', type=str, required=True,
                       help='Input directory path to process')
    parser.add_argument('-c', '--column-index', type=int, default=0,
                       help='Column index to remove (default: 0 - first column)')
    parser.add_argument('-o', '--old-char', type=str, default='ԥ',
                       help='Character to replace (default: ԥ)')
    parser.add_argument('-n', '--new-char', type=str, default='豫',
                       help='Replacement character (default: 豫)')
    parser.add_argument('--mp-workers', type=int, default=None,
                       help='Number of multiprocessing workers for column removal (default: auto)')
    parser.add_argument('--thread-workers', type=int, default=None,
                       help='Number of thread workers for character replacement (default: auto)')
    parser.add_argument('--skip-column-removal', action='store_true',
                       help='Skip phase 1 (column removal)')
    parser.add_argument('--skip-character-replacement', action='store_true',
                       help='Skip phase 2 (character replacement)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without making changes')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Show detailed processing information')
    
    return parser.parse_args()


def generate_processed_dir_name(input_dir: Path) -> Path:
    """Generate processed directory name"""
    input_folder_name = input_dir.name
    processed_folder_name = f"{input_folder_name}_处理后"
    processed_dir = input_dir.parent / processed_folder_name
    return processed_dir


def copy_files_to_processed_dir(input_dir: Path, processed_dir: Path, verbose: bool = False) -> int:
    """Copy files from input to processed directory"""
    if processed_dir.exists():
        if verbose:
            print(f"Processed directory already exists: {processed_dir}")
        return 0
    
    # Find all files to copy
    input_files = list(input_dir.glob("*"))
    files_to_copy = [f for f in input_files if f.is_file()]
    
    if not files_to_copy:
        return 0
    
    print(f"📁 Creating processed directory: {processed_dir}")
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    copied_count = 0
    print(f"📋 Copying {len(files_to_copy)} files to processed directory...")
    
    with tqdm(files_to_copy, desc="📋 Copying files", unit="files", ncols=100) as pbar:
        for file_path in pbar:
            try:
                dest_file = processed_dir / file_path.name
                shutil.copy2(file_path, dest_file)
                copied_count += 1
                if verbose:
                    tqdm.write(f"Copied: {file_path.name}")
            except Exception as e:
                if verbose:
                    tqdm.write(f"❌ Failed to copy {file_path.name}: {e}")
            
            pbar.set_postfix({'Copied': copied_count})
    
    print(f"✅ Copied {copied_count} files")
    return copied_count


def main():
    """Main function"""
    start_time = time.time()
    args = parse_arguments()
    
    # Parse arguments
    input_dir = Path(args.input).resolve()
    processed_dir = generate_processed_dir_name(input_dir)
    column_index = args.column_index
    old_char = args.old_char
    new_char = args.new_char
    mp_workers = args.mp_workers
    thread_workers = args.thread_workers
    skip_column_removal = args.skip_column_removal
    skip_character_replacement = args.skip_character_replacement
    dry_run = args.dry_run
    verbose = args.verbose
    
    print("🚀 === Integrated CSV Processor ===")
    print(f"Input directory: {input_dir}")
    print(f"Processed directory: {processed_dir}")
    print(f"Column to remove: {column_index}")
    print(f"Character replacement: '{old_char}' → '{new_char}'")
    
    if skip_column_removal:
        print("⚠️  Phase 1 (column removal) will be SKIPPED")
    if skip_character_replacement:
        print("⚠️  Phase 2 (character replacement) will be SKIPPED")
    if dry_run:
        print("🧪 DRY RUN MODE - No changes will be made")
    print()
    
    # Validate input directory
    if not input_dir.exists():
        print(f"❌ Error: Input directory does not exist: {input_dir}")
        return 1
    
    if not input_dir.is_dir():
        print(f"❌ Error: Input path is not a directory: {input_dir}")
        return 1
    
    # Initialize stats
    stats = ProcessingStats()
    
    if dry_run:
        print("🧪 === DRY RUN SUMMARY ===")
        csv_files = list(input_dir.rglob('*.csv'))
        print(f"Would process {len(csv_files)} CSV files for column removal")
        
        all_files = list(input_dir.rglob("*"))
        files_needing_char_replacement = [f for f in all_files if f.is_file() and (old_char in f.name or f.suffix.lower() == '.csv')]
        print(f"Would process {len(files_needing_char_replacement)} files for character replacement")
        return 0
    
    # Copy files to processed directory if needed
    copied_files = copy_files_to_processed_dir(input_dir, processed_dir, verbose)
    
    # Phase 1: Column Removal
    if not skip_column_removal:
        phase1_results = phase1_column_removal(processed_dir, column_index, mp_workers, verbose)
        stats.total_files = phase1_results['total']
        stats.column_removal_success = phase1_results['success']
        stats.column_removal_failed = phase1_results['failed']
    else:
        print("⏩ Skipping Phase 1 (column removal)")
    
    print()
    
    # Phase 2: Character Replacement
    if not skip_character_replacement:
        phase2_results = phase2_character_replacement(processed_dir, old_char, new_char, thread_workers, verbose)
        stats.character_replacement_success = phase2_results['success']
        stats.character_replacement_failed = phase2_results['failed']
        stats.files_renamed = phase2_results['renamed']
    else:
        print("⏩ Skipping Phase 2 (character replacement)")
    
    # Final summary
    total_time = time.time() - start_time
    stats.processing_time = total_time
    
    print(f"\n📊 === Final Processing Summary ===")
    print(f"⏱️  Total processing time: {total_time:.2f} seconds")
    print(f"📁 Files copied: {copied_files}")
    
    if not skip_column_removal:
        print(f"🗑️  Column removal - Success: {stats.column_removal_success}, Failed: {stats.column_removal_failed}")
    
    if not skip_character_replacement:
        print(f"🔤 Character replacement - Success: {stats.character_replacement_success}, Failed: {stats.character_replacement_failed}")
        print(f"📝 Files renamed: {stats.files_renamed}")
    
    print(f"✅ === Processing completed successfully ===")
    return 0


if __name__ == "__main__":
    exit(main())