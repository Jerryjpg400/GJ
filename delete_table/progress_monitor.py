#!/usr/bin/env python3
"""
Progress Monitor - Real-time progress visualization
Optional component for monitoring processing progress
"""

import sys
import time
import json
import os
from datetime import datetime


class ProgressBar:
    """Simple progress bar implementation."""
    
    def __init__(self, total, width=40):
        self.total = total
        self.width = width
        self.current = 0
        self.start_time = time.time()
        
    def update(self, count):
        """Update progress bar."""
        self.current = count
        self._draw()
        
    def _draw(self):
        """Draw progress bar to stdout."""
        percent = self.current / self.total if self.total > 0 else 0
        filled = int(self.width * percent)
        bar = '█' * filled + '░' * (self.width - filled)
        
        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        eta = (self.total - self.current) / rate if rate > 0 else 0
        
        print(f'\r[{bar}] {percent*100:.1f}% | '
              f'{self.current}/{self.total} | '
              f'{rate:.0f} files/s | '
              f'ETA: {int(eta//60)}:{int(eta%60):02d}', 
              end='', flush=True)


def monitor_checkpoint(checkpoint_file, total_files=None):
    """Monitor progress from checkpoint file."""
    
    if not os.path.exists(checkpoint_file):
        print(f"Checkpoint file not found: {checkpoint_file}")
        return
    
    # If total not provided, try to estimate
    if total_files is None:
        print("Counting total files...")
        directory = os.path.dirname(checkpoint_file) or '.'
        total_files = sum(1 for _ in glob.glob(os.path.join(directory, '**', '*.csv'), recursive=True))
    
    progress = ProgressBar(total_files)
    last_count = 0
    
    print(f"Monitoring progress (Total: {total_files} files)")
    
    try:
        while True:
            if os.path.exists(checkpoint_file):
                with open(checkpoint_file, 'r') as f:
                    data = json.load(f)
                    processed = len(data.get('processed', []))
                    
                    if processed != last_count:
                        progress.update(processed)
                        last_count = processed
                        
                        if processed >= total_files:
                            print("\nProcessing completed!")
                            break
            
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")


def generate_report(checkpoint_file, output_file=None):
    """Generate processing report from checkpoint."""
    
    if not os.path.exists(checkpoint_file):
        print(f"Checkpoint file not found: {checkpoint_file}")
        return
    
    with open(checkpoint_file, 'r') as f:
        data = json.load(f)
    
    processed = data.get('processed', [])
    timestamp = data.get('timestamp', 'Unknown')
    
    report = f"""CSV Processing Report
====================
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Checkpoint: {checkpoint_file}
Last Updated: {timestamp}

Total Processed: {len(processed)}

File List:
----------
"""
    
    for file in sorted(processed):
        report += f"{file}\n"
    
    if output_file:
        with open(output_file, 'w') as f:
            f.write(report)
        print(f"Report saved to: {output_file}")
    else:
        print(report)


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: progress_monitor.py <command> [args]")
        print("Commands:")
        print("  monitor <checkpoint_file> [total_files] - Monitor real-time progress")
        print("  report <checkpoint_file> [output_file] - Generate report")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'monitor':
        if len(sys.argv) < 3:
            print("Usage: progress_monitor.py monitor <checkpoint_file> [total_files]")
            sys.exit(1)
        checkpoint_file = sys.argv[2]
        total_files = int(sys.argv[3]) if len(sys.argv) > 3 else None
        monitor_checkpoint(checkpoint_file, total_files)
        
    elif command == 'report':
        if len(sys.argv) < 3:
            print("Usage: progress_monitor.py report <checkpoint_file> [output_file]")
            sys.exit(1)
        checkpoint_file = sys.argv[2]
        output_file = sys.argv[3] if len(sys.argv) > 3 else None
        generate_report(checkpoint_file, output_file)
        
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # Import glob only if needed
    if len(sys.argv) > 1 and sys.argv[1] == 'monitor':
        import glob
    main()