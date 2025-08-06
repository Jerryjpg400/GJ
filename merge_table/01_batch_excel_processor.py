#!/usr/bin/env python3
"""
批量Excel数据处理脚本 - 增强版
功能：
1. 支持单文件和批量处理
2. 并行化处理提升性能
3. 灵活的日期格式清理
4. 遵循Unix设计哲学
"""

import pandas as pd
import re
import sys
import argparse
import time
from pathlib import Path
from multiprocessing import Pool, cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

def clean_date_format(date_str):
    """增强的日期格式清理函数"""
    if pd.isna(date_str) or not date_str:
        return date_str
    
    # 支持多种日期格式：年/月/日
    patterns = [
        r'(\d{4}/\d{1,2}/\d{1,2})',  # 2025/1/3, 2025/11/30
        r'(\d{4}-\d{1,2}-\d{1,2})',  # 2025-1-3, 2025-11-30  
        r'(\d{1,2}/\d{1,2}/\d{4})'   # 1/3/2025, 11/30/2025
    ]
    
    date_string = str(date_str)
    
    for pattern in patterns:
        match = re.search(pattern, date_string)
        if match:
            date_part = match.group(1)
            # 统一转换为年/月/日格式
            if '/' in date_part and len(date_part.split('/')[0]) == 4:
                return date_part  # 已经是年/月/日格式
            elif '-' in date_part:
                return date_part.replace('-', '/')  # 转换年-月-日为年/月/日
            elif '/' in date_part and len(date_part.split('/')[2]) == 4:
                # 月/日/年转换为年/月/日
                parts = date_part.split('/')
                return f"{parts[2]}/{parts[0]}/{parts[1]}"
    
    return date_str

def process_single_excel(file_info):
    """处理单个Excel文件的函数，用于并行处理"""
    input_file, output_dir, keep_columns = file_info
    
    try:
        # 读取Excel文件
        df = pd.read_excel(input_file)
        original_shape = df.shape
        
        # 保留指定列数
        if keep_columns > 0:
            df_processed = df.iloc[:, 0:keep_columns].copy()
        else:
            df_processed = df.copy()
        
        # 清理日期格式
        date_columns = ['日期', 'date', 'Date', 'DATE']
        for col in df_processed.columns:
            if any(date_keyword in col for date_keyword in date_columns):
                df_processed[col] = df_processed[col].apply(clean_date_format)
                break
        
        # 生成输出文件名
        input_path = Path(input_file)
        if output_dir:
            output_path = Path(output_dir) / f"{input_path.stem}_processed{input_path.suffix}"
        else:
            output_path = input_path.parent / f"{input_path.stem}_processed{input_path.suffix}"
        
        # 确保输出目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存处理后的文件
        df_processed.to_excel(output_path, index=False)
        
        result = {
            'file': str(input_file),
            'success': True,
            'original_shape': original_shape,
            'processed_shape': df_processed.shape,
            'output': str(output_path),
            'columns': list(df_processed.columns)
        }
        
        return result
        
    except Exception as e:
        return {
            'file': str(input_file),
            'success': False,
            'error': str(e),
            'original_shape': None,
            'processed_shape': None,
            'output': None,
            'columns': None
        }

def find_excel_files(directory, pattern="*.xlsx"):
    """查找目录中的Excel文件"""
    directory = Path(directory)
    if not directory.exists():
        return []
    
    excel_files = []
    for pattern_ext in ["*.xlsx", "*.xls"]:
        excel_files.extend(directory.glob(pattern_ext))
        excel_files.extend(directory.glob(f"**/{pattern_ext}"))  # 递归查找
    
    return [f for f in excel_files if not f.name.startswith('~$')]  # 排除临时文件

def batch_process_excel(input_path, output_dir=None, keep_columns=5, max_workers=None, recursive=False):
    """批量处理Excel文件"""
    
    input_path = Path(input_path)
    
    # 确定要处理的文件列表
    if input_path.is_file():
        files_to_process = [input_path]
    elif input_path.is_dir():
        if recursive:
            files_to_process = find_excel_files(input_path)
        else:
            files_to_process = [f for f in input_path.glob("*.xlsx") if not f.name.startswith('~$')]
            files_to_process.extend([f for f in input_path.glob("*.xls") if not f.name.startswith('~$')])
    else:
        print(f"错误：路径 {input_path} 不存在")
        return False
    
    if not files_to_process:
        print(f"在 {input_path} 中未找到Excel文件")
        return False
    
    print(f"找到 {len(files_to_process)} 个Excel文件待处理")
    
    # 准备并行处理参数
    if max_workers is None:
        max_workers = min(cpu_count(), len(files_to_process))
    
    file_info_list = [(f, output_dir, keep_columns) for f in files_to_process]
    
    # 并行处理
    start_time = time.time()
    results = []
    
    if len(files_to_process) == 1:
        # 单文件处理，不使用并行
        print("处理单个文件...")
        result = process_single_excel(file_info_list[0])
        results = [result]
    else:
        # 多文件并行处理
        print(f"启动 {max_workers} 个进程进行并行处理...")
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(process_single_excel, file_info): file_info[0] 
                for file_info in file_info_list
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        print(f"✓ {Path(file_path).name}: {result['original_shape']} → {result['processed_shape']}")
                    else:
                        print(f"✗ {Path(file_path).name}: {result['error']}")
                        
                except Exception as e:
                    print(f"✗ {Path(file_path).name}: 处理异常 - {e}")
                    results.append({
                        'file': str(file_path),
                        'success': False,
                        'error': str(e)
                    })
    
    # 统计结果
    end_time = time.time()
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    
    print(f"\n处理完成！")
    print(f"总文件数: {len(results)}")
    print(f"成功: {successful}")
    print(f"失败: {failed}")
    print(f"处理时间: {end_time - start_time:.2f}秒")
    
    # 显示失败的文件
    if failed > 0:
        print("\n失败的文件:")
        for result in results:
            if not result['success']:
                print(f"  - {Path(result['file']).name}: {result.get('error', '未知错误')}")
    
    return successful > 0

def main():
    """主函数，支持命令行参数"""
    parser = argparse.ArgumentParser(
        description="批量Excel数据处理工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python %(prog)s data/0103.xlsx                    # 处理单个文件
  python %(prog)s data/                             # 处理目录中的所有Excel文件
  python %(prog)s data/ -o output/                  # 指定输出目录
  python %(prog)s data/ -c 3                       # 只保留前3列
  python %(prog)s data/ -w 8                       # 使用8个进程并行处理
  python %(prog)s data/ -r                         # 递归处理子目录
        """
    )
    
    parser.add_argument('input', help='输入文件或目录路径')
    parser.add_argument('-o', '--output', help='输出目录（可选）')
    parser.add_argument('-c', '--columns', type=int, default=5, 
                       help='保留的列数（默认：5，0表示保留所有列）')
    parser.add_argument('-w', '--workers', type=int, 
                       help='并行处理的进程数（默认：自动检测）')
    parser.add_argument('-r', '--recursive', action='store_true',
                       help='递归处理子目录')
    
    args = parser.parse_args()
    
    success = batch_process_excel(
        input_path=args.input,
        output_dir=args.output,
        keep_columns=args.columns,
        max_workers=args.workers,
        recursive=args.recursive
    )
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()