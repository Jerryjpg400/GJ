#!/usr/bin/env python3
"""
Batch CSV Processor - Main coordinator
Combines all tools to process CSV files efficiently
"""

import os
import sys
import time
import argparse
import subprocess
from pathlib import Path


def run_command(cmd, capture_output=False):
    """Run shell command."""
    if capture_output:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode, result.stdout, result.stderr
    else:
        return subprocess.run(cmd, shell=True).returncode, None, None


def main():
    """Main batch processor."""
    parser = argparse.ArgumentParser(description='Batch process CSV files to remove column')
    parser.add_argument('directory', help='Directory containing CSV files')
    parser.add_argument('-w', '--workers', type=int, default=None, 
                        help='Number of parallel workers (default: auto)')
    parser.add_argument('-c', '--checkpoint', type=str, default='checkpoint.json',
                        help='Checkpoint file for resume capability')
    parser.add_argument('-m', '--monitor', action='store_true',
                        help='Run progress monitor in background')
    parser.add_argument('-r', '--report', type=str, default=None,
                        help='Generate report after completion')
    parser.add_argument('--column-index', type=int, default=0,
                        help='Column index to remove (default: 0)')
    
    args = parser.parse_args()
    
    # Validate directory
    if not os.path.isdir(args.directory):
        print(f"Error: Directory not found: {args.directory}", file=sys.stderr)
        sys.exit(1)
    
    # Make sure scripts are executable
    scripts = ['csv_processor.py', 'parallel_processor.py', 'progress_monitor.py']
    for script in scripts:
        if os.path.exists(script):
            os.chmod(script, 0o755)
    
    print("CSV Batch Processor")
    print("===================")
    print(f"Directory: {args.directory}")
    print(f"Workers: {args.workers or 'auto'}")
    print(f"Checkpoint: {args.checkpoint}")
    print(f"Column to remove: index {args.column_index}")
    print()
    
    # Start progress monitor if requested
    monitor_process = None
    if args.monitor:
        # Count total files first
        total_files = sum(1 for _ in Path(args.directory).rglob('*.csv'))
        print(f"Total CSV files: {total_files}")
        
        # Start monitor in background
        monitor_cmd = f"{sys.executable} progress_monitor.py monitor {args.checkpoint} {total_files}"
        monitor_process = subprocess.Popen(monitor_cmd, shell=True)
        print("Progress monitor started in background")
        print()
    
    # Run parallel processor
    start_time = time.time()
    
    worker_arg = f" {args.workers}" if args.workers else ""
    cmd = f"{sys.executable} parallel_processor.py {args.directory}{worker_arg} {args.checkpoint}"
    
    print("Starting parallel processing...")
    print("-" * 50)
    
    returncode = run_command(cmd)[0]
    
    print("-" * 50)
    print(f"\nProcessing completed in {time.time() - start_time:.1f} seconds")
    
    # Stop monitor if running
    if monitor_process:
        monitor_process.terminate()
        monitor_process.wait()
    
    # Generate report if requested
    if args.report:
        print(f"\nGenerating report: {args.report}")
        report_cmd = f"{sys.executable} progress_monitor.py report {args.checkpoint} {args.report}"
        run_command(report_cmd)
    
    return returncode


if __name__ == "__main__":
    sys.exit(main())