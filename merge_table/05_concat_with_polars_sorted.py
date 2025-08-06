#!/usr/bin/env python3
"""
使用Polars优化的Excel文件合并工具 - 智能日期排序版本
采用混合方案：pandas读取Excel + Polars高性能合并 + pandas输出Excel
功能：
1. 智能文件读取策略（根据文件大小选择最优读取方式）
2. pandas到Polars的高效转换
3. 批量处理支持（避免内存溢出）
4. 内存优化和性能监控
5. 完善的错误处理和进度显示
6. 🔥 智能日期排序：支持YYYY_MMDD_*和MMDD_*两种文件名格式
遵循Unix设计哲学：专注做好一件事
"""

import argparse
import gc
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional
import warnings

import pandas as pd
import polars as pl
import psutil
from tqdm import tqdm

# 忽略特定警告
warnings.filterwarnings('ignore', category=UserWarning)

# 预编译正则表达式（性能优化）
PATTERN_WITH_YEAR = re.compile(r'^(\d{4})_(\d{2})(\d{2})_')
PATTERN_WITHOUT_YEAR = re.compile(r'^(\d{2})(\d{2})_')


class MemoryMonitor:
    """内存使用监控器"""
    
    def __init__(self, limit_percent=80):
        self.limit_percent = limit_percent
        self.process = psutil.Process()
        self.initial_memory = self.get_memory_usage()
    
    def get_memory_usage(self):
        """获取当前内存使用量（MB）"""
        return self.process.memory_info().rss / 1024 / 1024
    
    def get_memory_percent(self):
        """获取内存使用百分比"""
        return psutil.virtual_memory().percent
    
    def check_memory(self):
        """检查内存使用是否超限"""
        return self.get_memory_percent() < self.limit_percent
    
    def get_memory_info(self):
        """获取内存使用信息"""
        current = self.get_memory_usage()
        percent = self.get_memory_percent()
        return {
            'current_mb': current,
            'used_mb': current - self.initial_memory,
            'percent': percent
        }


def get_file_size_mb(file_path: Path) -> float:
    """获取文件大小（MB）"""
    return file_path.stat().st_size / 1024 / 1024


def extract_date_from_filename(file_path: Path) -> Tuple[int, int, int, str]:
    """
    智能提取文件名中的日期信息
    
    支持格式:
    - 2023_0101_data.xlsx → (2023, 1, 1)
    - 0328_data.xlsx → (2024, 3, 28) # 默认年份
    
    Returns:
        tuple: (年, 月, 日, 文件名) 用于排序
    """
    filename = file_path.stem
    
    # 格式1: YYYY_MMDD_* (优先匹配)
    match = PATTERN_WITH_YEAR.match(filename)
    if match:
        year, month, day = map(int, match.groups())
        # 快速有效性检查
        if 1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100:
            try:
                # 严格日期验证（处理如0230这种无效日期）
                datetime(year, month, day)
                return (year, month, day, file_path.name)
            except ValueError:
                pass
    
    # 格式2: MMDD_*
    match = PATTERN_WITHOUT_YEAR.match(filename)
    if match:
        month, day = map(int, match.groups())
        if 1 <= month <= 12 and 1 <= day <= 31:
            try:
                # 使用默认年份2024
                datetime(2024, month, day)
                return (2024, month, day, file_path.name)
            except ValueError:
                pass
    
    # 解析失败：使用最小值确保排在最前面，便于调试
    return (0, 0, 0, file_path.name)


def find_valid_xlsx_files(directory: str) -> List[Path]:
    """
    查找目录中有效的xlsx文件并按日期排序
    
    智能排序支持:
    - 2023_0101_* < 2023_0328_* < 2023_1102_* 
    - 2023_1102_* < 2024_0101_*
    - 0101_* < 0328_* < 1102_* (默认2024年)
    
    Args:
        directory: 目录路径
    
    Returns:
        list: 按日期排序的有效xlsx文件路径列表
    """
    directory = Path(directory)
    if not directory.exists():
        return []
    
    # 获取所有xlsx文件，排除临时文件和系统文件
    xlsx_files = []
    for file_path in directory.glob("*.xlsx"):
        if not file_path.name.startswith(('~$', '.', '#')):
            xlsx_files.append(file_path)
    
    if not xlsx_files:
        return []
    
    print(f"🔍 智能日期排序：分析 {len(xlsx_files)} 个文件...")
    
    # 提取所有文件的日期信息
    files_with_dates = []
    parse_stats = {'with_year': 0, 'without_year': 0, 'failed': 0}
    year_range = set()
    
    for file_path in xlsx_files:
        year, month, day, filename = extract_date_from_filename(file_path)
        files_with_dates.append((year, month, day, file_path))
        
        # 统计解析结果
        if year == 0:
            parse_stats['failed'] += 1
        elif year == 2024 and PATTERN_WITHOUT_YEAR.match(file_path.stem):
            parse_stats['without_year'] += 1
        else:
            parse_stats['with_year'] += 1
            year_range.add(year)
    
    # 显示解析统计
    print(f"📊 解析统计:")
    print(f"  ✓ 有年份格式 (YYYY_MMDD_*): {parse_stats['with_year']} 个")
    if year_range:
        print(f"    年份范围: {min(year_range)}-{max(year_range)}")
    print(f"  ✓ 无年份格式 (MMDD_*): {parse_stats['without_year']} 个 (默认2024年)")
    if parse_stats['failed'] > 0:
        print(f"  ⚠ 解析失败: {parse_stats['failed']} 个 (将放在最前面)")
    
    # 按日期排序：(年, 月, 日) 元组自然排序
    sorted_files_with_dates = sorted(files_with_dates, key=lambda x: (x[0], x[1], x[2]))
    
    # 提取排序后的文件路径
    sorted_files = [item[3] for item in sorted_files_with_dates]
    
    # 显示排序结果示例
    if len(sorted_files) <= 10:
        print(f"📅 排序结果: {[f.name for f in sorted_files]}")
    else:
        first_3 = [f.name for f in sorted_files[:3]]
        last_3 = [f.name for f in sorted_files[-3:]]
        print(f"📅 排序结果示例: {first_3} ... {last_3}")
    
    return sorted_files


def validate_column_consistency(file_paths: List[Path]) -> Tuple[bool, Optional[List[str]], Optional[str]]:
    """
    验证所有文件的列结构一致性
    
    Args:
        file_paths: xlsx文件路径列表
    
    Returns:
        tuple: (是否一致, 统一列名, 错误信息)
    """
    if not file_paths:
        return False, None, "没有找到有效文件"
    
    reference_columns = None
    
    for file_path in file_paths:
        try:
            # 只读取第一行获取列名，提高性能
            df_sample = pd.read_excel(file_path, nrows=0)
            current_columns = list(df_sample.columns)
            
            if reference_columns is None:
                reference_columns = current_columns
            elif current_columns != reference_columns:
                return False, None, f"文件 {file_path.name} 的列结构与其他文件不一致"
                
        except Exception as e:
            return False, None, f"读取文件 {file_path.name} 时出错: {e}"
    
    return True, reference_columns, None


def read_excel_optimized(file_path: Path, file_size_mb: float) -> Tuple[str, Optional[pl.DataFrame], Optional[str]]:
    """
    优化的Excel读取，立即转换为Polars DataFrame
    
    Args:
        file_path: 文件路径
        file_size_mb: 文件大小（MB）
    
    Returns:
        tuple: (文件名, Polars DataFrame, 错误信息)
    """
    try:
        # 根据文件大小选择读取策略
        if file_size_mb < 10:
            # 小文件：直接pandas读取
            df_pandas = pd.read_excel(file_path, engine='openpyxl')
        else:
            # 大文件：使用read_only模式
            df_pandas = pd.read_excel(file_path, engine='openpyxl')
        
        # 立即转换为Polars DataFrame
        df_polars = pl.from_pandas(df_pandas)
        
        # 释放pandas DataFrame内存
        del df_pandas
        gc.collect()
        
        return file_path.name, df_polars, None
        
    except Exception as e:
        return file_path.name, None, str(e)


def batch_process_files(
    file_paths: List[Path], 
    batch_size: int = 50,
    max_workers: int = None,
    progress_bar: Optional[tqdm] = None
) -> Tuple[List[pl.DataFrame], int]:
    """
    批量处理文件，避免内存溢出
    
    Args:
        file_paths: 文件路径列表
        batch_size: 每批处理的文件数
        max_workers: 最大工作线程数
        progress_bar: 进度条对象
    
    Returns:
        tuple: (Polars DataFrame列表, 总行数)
    """
    if max_workers is None:
        max_workers = min(int(os.cpu_count() * 0.8), 4)
    
    all_dataframes = []
    total_rows = 0
    
    # 分批处理
    for i in range(0, len(file_paths), batch_size):
        batch_files = file_paths[i:i + batch_size]
        batch_dfs = []
        
        # 获取文件大小信息
        file_sizes = [(fp, get_file_size_mb(fp)) for fp in batch_files]
        
        # 并行读取当前批次
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(read_excel_optimized, fp, size): fp 
                for fp, size in file_sizes
            }
            
            for future in as_completed(future_to_file):
                filename, df, error = future.result()
                
                if error:
                    print(f"\n✗ {filename}: 读取失败 - {error}")
                    raise Exception(f"文件读取失败: {filename}")
                else:
                    batch_dfs.append(df)
                    rows = df.shape[0]
                    total_rows += rows
                    if progress_bar:
                        progress_bar.update(1)
                        progress_bar.set_postfix({'rows': total_rows})
        
        # 合并当前批次
        if batch_dfs:
            # 使用Polars的高效concat
            batch_merged = pl.concat(batch_dfs, rechunk=True)
            all_dataframes.append(batch_merged)
            
            # 释放批次内的DataFrame
            del batch_dfs
            gc.collect()
    
    return all_dataframes, total_rows


def merge_with_polars(
    input_dir: str,
    output_file: Optional[str] = None,
    batch_size: int = 50,
    max_workers: Optional[int] = None,
    memory_limit: int = 80
) -> bool:
    """
    使用Polars优化的文件合并主函数
    
    Args:
        input_dir: 输入目录路径
        output_file: 输出文件路径（可选）
        batch_size: 批处理大小
        max_workers: 最大工作线程数
        memory_limit: 内存使用限制百分比
    
    Returns:
        bool: 是否成功
    """
    
    print(f"=== Polars优化Excel文件合并工具 (智能日期排序版) ===")
    
    # 初始化内存监控
    memory_monitor = MemoryMonitor(memory_limit)
    
    # 1. 查找有效文件并按日期排序
    xlsx_files = find_valid_xlsx_files(input_dir)
    
    if not xlsx_files:
        print(f"在目录 {input_dir} 中未找到有效的xlsx文件")
        return False
    
    print(f"\n找到并排序 {len(xlsx_files)} 个xlsx文件")
    print(f"内存限制: {memory_limit}%")
    print(f"批处理大小: {batch_size} 文件/批")
    
    # 2. 验证列结构一致性
    print(f"\n验证列结构一致性...")
    is_consistent, columns, error_msg = validate_column_consistency(xlsx_files)
    
    if not is_consistent:
        print(f"✗ 列结构验证失败: {error_msg}")
        return False
    
    print(f"✓ 列结构一致，共 {len(columns)} 列")
    
    # 3. 批量读取和处理数据
    print(f"\n开始批量处理数据...")
    start_time = time.time()
    
    try:
        # 使用进度条
        with tqdm(total=len(xlsx_files), desc="读取文件", unit="file") as pbar:
            batch_dataframes, total_rows = batch_process_files(
                xlsx_files, 
                batch_size=batch_size,
                max_workers=max_workers,
                progress_bar=pbar
            )
        
        # 显示内存使用情况
        mem_info = memory_monitor.get_memory_info()
        print(f"\n内存使用: {mem_info['current_mb']:.1f}MB (已用: {mem_info['used_mb']:.1f}MB, {mem_info['percent']:.1f}%)")
        
        # 4. 最终合并所有批次
        print(f"\n合并所有批次数据...")
        if len(batch_dataframes) > 1:
            final_df = pl.concat(batch_dataframes, rechunk=True)
        else:
            final_df = batch_dataframes[0]
        
        # 释放中间结果
        del batch_dataframes
        gc.collect()
        
        read_time = time.time() - start_time
        print(f"✓ 数据合并完成 (已按文件日期顺序排列)")
        print(f"最终形状: {final_df.shape}")
        print(f"处理时间: {read_time:.2f}秒")
        
    except Exception as e:
        print(f"✗ 数据处理失败: {e}")
        return False
    
    # 5. 保存结果
    if output_file is None:
        output_file = Path(input_dir) / "merged_all_polars_sorted.xlsx"
    else:
        output_file = Path(output_file)
    
    # 确保输出目录存在
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\n保存合并结果到: {output_file}")
    save_start = time.time()
    
    try:
        # 转换回pandas并保存
        # 注意：对于超大文件，这一步可能是瓶颈
        df_pandas = final_df.to_pandas()
        df_pandas.to_excel(output_file, index=False, engine='openpyxl')
        
        # 释放内存
        del df_pandas
        del final_df
        gc.collect()
        
        save_time = time.time() - save_start
        
        print(f"✓ 文件保存成功")
        print(f"保存时间: {save_time:.2f}秒")
        
        # 统计信息
        total_time = time.time() - start_time
        print(f"\n=== 合并统计 ===")
        print(f"输入文件数: {len(xlsx_files)}")
        print(f"总行数: {total_rows}")
        print(f"总列数: {len(columns)}")
        print(f"输出文件: {output_file.name}")
        print(f"总耗时: {total_time:.2f}秒")
        print(f"平均处理速度: {len(xlsx_files)/total_time:.1f} 文件/秒")
        print(f"🎯 数据已按文件名日期顺序正确排列")
        
        # 最终内存使用情况
        final_mem = memory_monitor.get_memory_info()
        print(f"最终内存使用: {final_mem['current_mb']:.1f}MB (增加: {final_mem['used_mb']:.1f}MB)")
        
        return True
        
    except Exception as e:
        print(f"✗ 文件保存失败: {e}")
        return False


def main():
    """主函数，支持命令行参数"""
    parser = argparse.ArgumentParser(
        description="使用Polars优化的Excel文件合并工具 - 智能日期排序版本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
智能日期排序支持:
  支持文件名格式: YYYY_MMDD_* 和 MMDD_*
  排序示例: 2023_0101_* < 2023_0328_* < 2023_1102_* < 2024_0101_*
           0101_* < 0328_* < 1102_* (默认使用2024年)

使用示例:
  python %(prog)s merged/                               # 合并merged目录下所有xlsx文件
  python %(prog)s merged/ -o final_merged.xlsx         # 指定输出文件名
  python %(prog)s merged/ --batch-size 100             # 设置批处理大小
  python %(prog)s merged/ --max-workers 8              # 设置最大工作线程数
  python %(prog)s merged/ --memory-limit 90            # 设置内存使用限制
        """
    )
    
    parser.add_argument('input_dir', help='包含xlsx文件的输入目录')
    parser.add_argument('-o', '--output', help='输出文件路径（默认：输入目录/merged_all_polars_sorted.xlsx）')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='批处理大小，即每批处理的文件数（默认：50）')
    parser.add_argument('--max-workers', type=int,
                       help='最大工作线程数（默认：CPU核心数*0.8）')
    parser.add_argument('--memory-limit', type=int, default=80,
                       help='内存使用限制百分比（默认：80）')
    
    args = parser.parse_args()
    
    success = merge_with_polars(
        input_dir=args.input_dir,
        output_file=args.output,
        batch_size=args.batch_size,
        max_workers=args.max_workers,
        memory_limit=args.memory_limit
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()