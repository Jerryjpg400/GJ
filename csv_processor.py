#!/usr/bin/env python3
"""
CSV Column Remover - Unix Philosophy Implementation
Simple, focused tools that do one thing well
"""

import sys
import csv
import os
import tempfile
import shutil


def remove_column_from_csv(input_file, column_index=0):
    """Remove a column from CSV file by index. Simple and focused."""
    
    # Create temporary file in same directory for atomic operation
    temp_fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(input_file))
    
    try:
        with os.fdopen(temp_fd, 'w', newline='', encoding='utf-8') as temp_file:
            with open(input_file, 'r', newline='', encoding='utf-8-sig') as infile:
                reader = csv.reader(infile)
                writer = csv.writer(temp_file)
                
                for row in reader:
                    if len(row) > column_index:
                        row.pop(column_index)
                    writer.writerow(row)
        
        # Atomic replace
        shutil.move(temp_path, input_file)
        return True
        
    except Exception as e:
        # Clean up on error
        if os.path.exists(temp_path):
            os.remove(temp_path)
        print(f"Error processing {input_file}: {e}", file=sys.stderr)
        return False


def main():
    """Process single file from command line."""
    if len(sys.argv) < 2:
        print("Usage: csv_processor.py <csv_file> [column_index]", file=sys.stderr)
        sys.exit(1)
    
    input_file = sys.argv[1]
    column_index = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    
    if not os.path.exists(input_file):
        print(f"File not found: {input_file}", file=sys.stderr)
        sys.exit(1)
    
    success = remove_column_from_csv(input_file, column_index)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()