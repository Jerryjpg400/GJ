#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
import shutil
from pathlib import Path

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

def rename_file_with_character_replacement(file_path, old_char='ԥ', new_char='豫'):
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
            print(f"Renamed: {old_name} -> {new_name}")
            return str(new_path)
        
        return str(file_path)
        
    except Exception as e:
        print(f"Error renaming {file_path}: {e}")
        return str(file_path)

def main():
    # Define paths and characters
    base_dir = Path("/Users/cccc/Desktop/GJ")
    target_csv = base_dir / "ԥN00775D.csv"
    processed_dir = base_dir / "20231201_处理后"
    old_char = 'ԥ'
    new_char = '豫'
    
    print("=== Character Replacement Script ===")
    print(f"Replacing '{old_char}' with '{new_char}'")
    print()
    
    # Process the specific CSV file if it exists in root directory
    if target_csv.exists():
        print(f"Processing CSV content: {target_csv}")
        if replace_character_in_csv_content(target_csv, old_char, new_char):
            print("✓ CSV content updated successfully")
            
            # Rename the file
            new_path = rename_file_with_character_replacement(target_csv, old_char, new_char)
            print(f"✓ File renamed to: {Path(new_path).name}")
        else:
            print("✗ Failed to update CSV content")
    else:
        # Check if it exists in processed directory
        target_csv_in_processed = processed_dir / "ԥN00775D.csv"
        if target_csv_in_processed.exists():
            print(f"Processing CSV content: {target_csv_in_processed}")
            if replace_character_in_csv_content(target_csv_in_processed, old_char, new_char):
                print("✓ CSV content updated successfully")
                
                # Rename the file
                new_path = rename_file_with_character_replacement(target_csv_in_processed, old_char, new_char)
                print(f"✓ File renamed to: {Path(new_path).name}")
            else:
                print("✗ Failed to update CSV content")
    
    print()
    
    # Batch process all files in the processed directory
    if processed_dir.exists():
        print(f"Processing files in directory: {processed_dir}")
        
        # Find all files containing the old character
        files_to_process = []
        for file_path in processed_dir.rglob("*"):
            if file_path.is_file() and old_char in file_path.name:
                files_to_process.append(file_path)
        
        print(f"Found {len(files_to_process)} files to process")
        
        if files_to_process:
            print("Processing files...")
            
            success_count = 0
            for file_path in files_to_process:
                try:
                    # Rename file
                    new_path = rename_file_with_character_replacement(file_path, old_char, new_char)
                    success_count += 1
                    
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
            
            print(f"✓ Successfully processed {success_count}/{len(files_to_process)} files")
        else:
            print("No files found with the target character")
    else:
        print(f"Directory not found: {processed_dir}")
    
    print("\n=== Script completed ===")

if __name__ == "__main__":
    main()