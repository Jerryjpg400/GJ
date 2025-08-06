#!/usr/bin/env python3
"""
批量过滤Excel表格中特定里程范围的数据
遵循Unix设计哲学：做好一件事
支持单文件和批量文件夹处理，并行化提升性能
"""

import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd


def filter_single_file(file_info):
    """
    处理单个Excel文件的函数，用于并行处理
    
    Args:
        file_info: (input_file, output_dir, min_value, max_value, range_label)
    
    Returns:
        dict: 处理结果信息
    """
    input_file, output_dir, min_value, max_value, range_label = file_info
    
    try:
        # 读取Excel文件
        df = pd.read_excel(input_file)
        
        # 检查是否包含总里程列
        mileage_columns = ['总里程(公里)', '总里程', 'mileage', 'total_mileage']
        mileage_col = None
        
        for col in mileage_columns:
            if col in df.columns:
                mileage_col = col
                break
        
        if mileage_col is None:
            return {
                'file': str(input_file),
                'success': False,
                'error': f'未找到里程列，可用列: {list(df.columns)}',
                'range_label': range_label,
                'original_count': len(df),
                'filtered_count': 0
            }
        
        # 过滤数据
        filtered_df = df[(df[mileage_col] >= min_value) & 
                        (df[mileage_col] <= max_value)]
        
        # 生成输出文件名，按范围标签创建子文件夹
        input_path = Path(input_file)
        if output_dir:
            # 使用指定输出目录，在其下创建范围子文件夹
            output_path = Path(output_dir) / range_label / f"{input_path.stem}_{range_label}{input_path.suffix}"
        else:
            # 在输入文件同级目录创建范围子文件夹
            output_path = input_path.parent / range_label / f"{input_path.stem}_{range_label}{input_path.suffix}"
        
        # 确保输出目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存过滤后的数据
        filtered_df.to_excel(output_path, index=False, engine='openpyxl')
        
        return {
            'file': str(input_file),
            'success': True,
            'output': str(output_path),
            'range_label': range_label,
            'original_count': len(df),
            'filtered_count': len(filtered_df),
            'mileage_column': mileage_col
        }
        
    except Exception as e:
        return {
            'file': str(input_file),
            'success': False,
            'error': str(e),
            'range_label': range_label,
            'original_count': 0,
            'filtered_count': 0
        }


def find_excel_files(directory, recursive=False):
    """查找目录中的Excel文件"""
    directory = Path(directory)
    if not directory.exists():
        return []
    
    excel_files = []
    patterns = ["*.xlsx", "*.xls"]
    
    for pattern in patterns:
        if recursive:
            excel_files.extend(directory.glob(f"**/{pattern}"))
        else:
            excel_files.extend(directory.glob(pattern))
    
    # 排除临时文件，但保留已处理的文件
    return [f for f in excel_files if not f.name.startswith('~$')]


def batch_filter_mileage(input_path, ranges, output_dir=None, max_workers=None, recursive=False):
    """
    批量过滤Excel文件中的里程数据
    
    Args:
        input_path: 输入文件或目录路径
        ranges: 过滤范围列表，格式: [(min1, max1, label1), (min2, max2, label2), ...]
        output_dir: 输出目录（可选）
        max_workers: 并行处理的进程数
        recursive: 是否递归处理子目录
    """
    
    input_path = Path(input_path)
    
    # 确定要处理的文件列表
    if input_path.is_file():
        files_to_process = [input_path]
    elif input_path.is_dir():
        files_to_process = find_excel_files(input_path, recursive)
    else:
        print(f"错误：路径 {input_path} 不存在")
        return False
    
    if not files_to_process:
        print(f"在 {input_path} 中未找到Excel文件")
        return False
    
    print(f"找到 {len(files_to_process)} 个Excel文件")
    print(f"过滤范围: {[f'[{r[0]}-{r[1]}]({r[2]})' for r in ranges]}")
    
    # 准备并行处理任务
    if max_workers is None:
        max_workers = min(cpu_count(), len(files_to_process) * len(ranges))
    
    # 为每个文件和每个范围生成任务
    tasks = []
    for file_path in files_to_process:
        for min_val, max_val, label in ranges:
            tasks.append((file_path, output_dir, min_val, max_val, label))
    
    print(f"启动 {max_workers} 个进程处理 {len(tasks)} 个任务...")
    
    # 并行处理
    start_time = time.time()
    results = []
    
    if len(tasks) == 1:
        # 单任务处理
        result = filter_single_file(tasks[0])
        results = [result]
    else:
        # 多任务并行处理
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(filter_single_file, task): task 
                for task in tasks
            }
            
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        print(f"✓ {Path(result['file']).name} [{result['range_label']}]: "
                              f"{result['original_count']} → {result['filtered_count']} 条记录")
                    else:
                        print(f"✗ {Path(result['file']).name} [{result['range_label']}]: "
                              f"{result['error']}")
                        
                except Exception as e:
                    file_path, _, _, _, range_label = task
                    print(f"✗ {Path(file_path).name} [{range_label}]: 处理异常 - {e}")
                    results.append({
                        'file': str(file_path),
                        'success': False,
                        'error': str(e),
                        'range_label': range_label
                    })
    
    # 统计结果
    end_time = time.time()
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    
    print(f"\n处理完成！")
    print(f"总任务数: {len(results)}")
    print(f"成功: {successful}")
    print(f"失败: {failed}")
    print(f"处理时间: {end_time - start_time:.2f}秒")
    
    # 按范围汇总统计
    range_stats = {}
    for result in results:
        if result['success']:
            label = result['range_label']
            if label not in range_stats:
                range_stats[label] = {'files': 0, 'total_filtered': 0}
            range_stats[label]['files'] += 1
            range_stats[label]['total_filtered'] += result['filtered_count']
    
    if range_stats:
        print("\n各范围统计:")
        for label, stats in range_stats.items():
            print(f"  {label}: {stats['files']} 个文件，共过滤出 {stats['total_filtered']} 条记录")
    
    # 显示失败的任务
    if failed > 0:
        print("\n失败的任务:")
        for result in results:
            if not result['success']:
                print(f"  - {Path(result['file']).name} [{result['range_label']}]: "
                      f"{result.get('error', '未知错误')}")
    
    return successful > 0


def main():
    """主函数，支持命令行参数"""
    parser = argparse.ArgumentParser(
        description="批量过滤Excel文件中的里程数据",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python %(prog)s data/0103.xlsx                        # 处理单个文件，使用默认范围
  python %(prog)s data/                                 # 处理目录中的所有Excel文件
  python %(prog)s data/ -o output/                      # 指定输出目录
  python %(prog)s data/ -r 0,20,low 45,55,medium       # 自定义过滤范围
  python %(prog)s data/ -w 8                           # 使用8个进程并行处理
  python %(prog)s data/ --recursive                    # 递归处理子目录
        """
    )
    
    parser.add_argument('input', help='输入文件或目录路径')
    parser.add_argument('-o', '--output', help='输出目录（可选）')
    parser.add_argument('-r', '--ranges', nargs='*', default=['0,30,low', '45,85,medium'],
                       help='过滤范围，格式: min,max,label（默认: 0,30,low 45,85,medium）')
    parser.add_argument('-w', '--workers', type=int,
                       help='并行处理的进程数（默认：自动检测）')
    parser.add_argument('--recursive', action='store_true',
                       help='递归处理子目录')
    
    args = parser.parse_args()
    
    # 解析过滤范围
    try:
        parsed_ranges = []
        for range_str in args.ranges:
            parts = range_str.split(',')
            if len(parts) != 3:
                raise ValueError(f"范围格式错误: {range_str}，应为: min,max,label")
            min_val, max_val, label = float(parts[0]), float(parts[1]), parts[2]
            parsed_ranges.append((min_val, max_val, label))
    except ValueError as e:
        print(f"参数错误: {e}")
        sys.exit(1)
    
    success = batch_filter_mileage(
        input_path=args.input,
        ranges=parsed_ranges,
        output_dir=args.output,
        max_workers=args.workers,
        recursive=args.recursive
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()