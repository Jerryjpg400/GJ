#!/usr/bin/env python3
"""
批量合并Low和Medium Excel文件
功能：
1. 匹配数字前缀相同的文件
2. 随机复制medium数据对齐至low数据量
3. 横向合并（low在左，medium在右）
4. 并行化处理
遵循Unix设计哲学：做好一件事
"""

import argparse
import sys
import time
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from pathlib import Path

import pandas as pd
import numpy as np


def extract_prefix(filename):
    """从文件名提取数字前缀"""
    return filename.stem.split('_')[0]


def find_matched_files(low_dir, medium_dir):
    """找到匹配的文件对"""
    low_dir = Path(low_dir)
    medium_dir = Path(medium_dir)
    
    # 获取所有Excel文件
    low_files = list(low_dir.glob("*.xlsx"))
    medium_files = list(medium_dir.glob("*.xlsx"))
    
    # 建立前缀到文件的映射
    low_map = {extract_prefix(f): f for f in low_files}
    medium_map = {extract_prefix(f): f for f in medium_files}
    
    # 找到匹配的前缀
    matched_prefixes = set(low_map.keys()) & set(medium_map.keys())
    
    # 返回匹配的文件对
    file_pairs = []
    for prefix in sorted(matched_prefixes):
        file_pairs.append((prefix, low_map[prefix], medium_map[prefix]))
    
    return file_pairs


def merge_single_pair(task_info):
    """
    处理单个文件对的合并任务
    
    Args:
        task_info: (prefix, low_file, medium_file, output_dir, random_seed)
    
    Returns:
        dict: 处理结果信息
    """
    prefix, low_file, medium_file, output_dir, random_seed = task_info
    
    try:
        # 设置随机种子以确保可重现性
        random.seed(random_seed)
        np.random.seed(random_seed)
        
        # 读取数据
        df_low = pd.read_excel(low_file)
        df_medium = pd.read_excel(medium_file)
        
        low_count = len(df_low)
        medium_count = len(df_medium)
        
        # 数据对齐：随机复制medium数据以匹配low数据量
        if medium_count < low_count:
            # 计算需要复制的数量
            needed_copies = low_count - medium_count
            
            # 随机复制medium数据
            # 使用replace=True允许重复采样
            additional_medium = df_medium.sample(n=needed_copies, replace=True, random_state=random_seed)
            
            # 合并原始数据和复制数据
            df_medium_aligned = pd.concat([df_medium, additional_medium], ignore_index=True)
            
            # 随机打乱顺序以确保随机化
            df_medium_aligned = df_medium_aligned.sample(frac=1, random_state=random_seed).reset_index(drop=True)
        else:
            df_medium_aligned = df_medium.copy()
        
        # 添加列前缀以区分数据来源
        df_low_prefixed = df_low.add_prefix('low_')
        df_medium_prefixed = df_medium_aligned.add_prefix('medium_')
        
        # 横向合并（low在左，medium在右）
        df_merged = pd.concat([df_low_prefixed, df_medium_prefixed], axis=1)
        
        # 生成输出文件名
        if output_dir:
            output_path = Path(output_dir) / f"{prefix}_merged.xlsx"
        else:
            output_path = Path(low_file).parent.parent / f"{prefix}_merged.xlsx"
        
        # 确保输出目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存合并结果
        df_merged.to_excel(output_path, index=False, engine='openpyxl')
        
        return {
            'prefix': prefix,
            'success': True,
            'low_file': str(low_file),
            'medium_file': str(medium_file),
            'output': str(output_path),
            'low_original_count': low_count,
            'medium_original_count': medium_count,
            'medium_aligned_count': len(df_medium_aligned),
            'merged_shape': df_merged.shape,
            'random_seed': random_seed
        }
        
    except Exception as e:
        return {
            'prefix': prefix,
            'success': False,
            'error': str(e),
            'low_file': str(low_file),
            'medium_file': str(medium_file),
            'random_seed': random_seed
        }


def batch_merge_files(low_dir, medium_dir, output_dir=None, max_workers=None, base_seed=42):
    """
    批量合并low和medium文件
    
    Args:
        low_dir: low文件目录
        medium_dir: medium文件目录
        output_dir: 输出目录（可选）
        max_workers: 并行处理的进程数
        base_seed: 基础随机种子
    """
    
    # 找到匹配的文件对
    file_pairs = find_matched_files(low_dir, medium_dir)
    
    if not file_pairs:
        print("未找到匹配的文件对")
        return False
    
    print(f"找到 {len(file_pairs)} 对匹配的文件:")
    for prefix, low_file, medium_file in file_pairs:
        print(f"  {prefix}: {low_file.name} + {medium_file.name}")
    
    # 准备并行处理任务
    if max_workers is None:
        max_workers = min(cpu_count(), len(file_pairs))
    
    # 为每个任务生成不同的随机种子
    tasks = []
    for i, (prefix, low_file, medium_file) in enumerate(file_pairs):
        task_seed = base_seed + i * 1000  # 确保不同任务有不同的种子
        tasks.append((prefix, low_file, medium_file, output_dir, task_seed))
    
    print(f"\n启动 {max_workers} 个进程处理 {len(tasks)} 个合并任务...")
    
    # 并行处理
    start_time = time.time()
    results = []
    
    if len(tasks) == 1:
        # 单任务处理
        result = merge_single_pair(tasks[0])
        results = [result]
    else:
        # 多任务并行处理
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(merge_single_pair, task): task 
                for task in tasks
            }
            
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        print(f"✓ {result['prefix']}: "
                              f"Low({result['low_original_count']}) + "
                              f"Medium({result['medium_original_count']}→{result['medium_aligned_count']}) = "
                              f"合并表({result['merged_shape'][0]}×{result['merged_shape'][1]})")
                    else:
                        print(f"✗ {result['prefix']}: {result['error']}")
                        
                except Exception as e:
                    prefix, _, _, _, seed = task
                    print(f"✗ {prefix}: 处理异常 - {e}")
                    results.append({
                        'prefix': prefix,
                        'success': False,
                        'error': str(e)
                    })
    
    # 统计结果
    end_time = time.time()
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    
    print(f"\n合并完成！")
    print(f"总任务数: {len(results)}")
    print(f"成功: {successful}")
    print(f"失败: {failed}")
    print(f"处理时间: {end_time - start_time:.2f}秒")
    
    # 显示成功任务的详细信息
    if successful > 0:
        print("\n合并详情:")
        for result in results:
            if result['success']:
                print(f"  {result['prefix']}:")
                print(f"    输出文件: {Path(result['output']).name}")
                print(f"    数据对齐: Medium {result['medium_original_count']} → {result['medium_aligned_count']} 行")
                print(f"    最终形状: {result['merged_shape'][0]} 行 × {result['merged_shape'][1]} 列")
                print(f"    随机种子: {result['random_seed']}")
    
    # 显示失败的任务
    if failed > 0:
        print("\n失败的任务:")
        for result in results:
            if not result['success']:
                print(f"  - {result['prefix']}: {result.get('error', '未知错误')}")
    
    return successful > 0


def main():
    """主函数，支持命令行参数"""
    parser = argparse.ArgumentParser(
        description="批量合并Low和Medium Excel文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python %(prog)s cleaned/low cleaned/medium                 # 基本合并
  python %(prog)s cleaned/low cleaned/medium -o merged/     # 指定输出目录
  python %(prog)s cleaned/low cleaned/medium -w 4           # 使用4个进程
  python %(prog)s cleaned/low cleaned/medium -s 123         # 指定随机种子
        """
    )
    
    parser.add_argument('low_dir', help='Low文件目录路径')
    parser.add_argument('medium_dir', help='Medium文件目录路径')
    parser.add_argument('-o', '--output', help='输出目录（可选）')
    parser.add_argument('-w', '--workers', type=int,
                       help='并行处理的进程数（默认：自动检测）')
    parser.add_argument('-s', '--seed', type=int, default=42,
                       help='随机种子（默认：42）')
    
    args = parser.parse_args()
    
    success = batch_merge_files(
        low_dir=args.low_dir,
        medium_dir=args.medium_dir,
        output_dir=args.output,
        max_workers=args.workers,
        base_seed=args.seed
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()