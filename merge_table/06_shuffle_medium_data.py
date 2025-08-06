#!/usr/bin/env python3
"""
混合方案数据打乱工具 - 使用Polars优化
功能：
1. 读取merged Excel文件（10列：前5列low_*，后5列medium_*）
2. 保持前5列数据不变
3. 将后5列数据按行随机打乱（最大化随机性）
4. 使用pandas读取+Polars处理+pandas保存的混合方案
5. 支持大数据集的高效处理

遵循Unix设计哲学：专注做好一件事
"""

import argparse
import gc
import sys
import time
from pathlib import Path
from typing import Tuple, Optional
import warnings

import pandas as pd
import polars as pl
import numpy as np
import psutil

# 忽略特定警告
warnings.filterwarnings('ignore', category=UserWarning)


class PerformanceMonitor:
    """性能监控器"""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = time.time()
        self.checkpoints = {}
        self.memory_peak = 0
        
    def checkpoint(self, name: str):
        """记录检查点"""
        current_time = time.time()
        current_memory = self.process.memory_info().rss / 1024 / 1024
        
        self.checkpoints[name] = {
            'time': current_time - self.start_time,
            'memory_mb': current_memory
        }
        
        if current_memory > self.memory_peak:
            self.memory_peak = current_memory
    
    def get_summary(self):
        """获取性能摘要"""
        return {
            'total_time': time.time() - self.start_time,
            'memory_peak_mb': self.memory_peak,
            'checkpoints': self.checkpoints
        }


def validate_file_structure(file_path: Path) -> Tuple[bool, Optional[str]]:
    """
    验证文件结构是否符合要求
    
    Args:
        file_path: Excel文件路径
    
    Returns:
        tuple: (是否有效, 错误信息)
    """
    try:
        # 只读取前几行验证结构
        df_sample = pd.read_excel(file_path, nrows=5)
        
        # 检查列数
        if df_sample.shape[1] != 10:
            return False, f"文件应有10列，实际有{df_sample.shape[1]}列"
        
        # 检查是否有数据
        total_rows = len(pd.read_excel(file_path, usecols=[0]))
        if total_rows == 0:
            return False, "文件中没有数据行"
        
        return True, None
        
    except Exception as e:
        return False, f"读取文件失败: {e}"


def load_and_validate_data(file_path: Path, monitor: PerformanceMonitor) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    加载并验证数据
    
    Args:
        file_path: Excel文件路径
        monitor: 性能监控器
    
    Returns:
        tuple: (DataFrame对象, 错误信息)
    """
    try:
        print(f"读取文件: {file_path}")
        
        # 验证文件结构
        is_valid, error_msg = validate_file_structure(file_path)
        if not is_valid:
            return None, error_msg
        
        # 读取完整数据
        df = pd.read_excel(file_path, engine='openpyxl')
        monitor.checkpoint('data_loaded')
        
        print(f"✓ 数据加载完成: {df.shape}")
        print(f"  前5列（low_*）: {list(df.columns[:5])}")
        print(f"  后5列（medium_*）: {list(df.columns[5:])}")
        
        return df, None
        
    except Exception as e:
        return None, f"数据加载失败: {e}"


def separate_data_columns(df_pandas: pd.DataFrame, monitor: PerformanceMonitor) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    分离前5列和后5列数据，转换为Polars DataFrame
    
    Args:
        df_pandas: pandas DataFrame
        monitor: 性能监控器
    
    Returns:
        tuple: (前5列Polars DF, 后5列Polars DF)
    """
    print("\n分离数据列...")
    
    # 分离前5列（low_* 数据，保持不变）
    low_data_pandas = df_pandas.iloc[:, :5].copy()
    low_data_polars = pl.from_pandas(low_data_pandas)
    
    # 分离后5列（medium_* 数据，需要打乱）
    medium_data_pandas = df_pandas.iloc[:, 5:].copy()
    medium_data_polars = pl.from_pandas(medium_data_pandas)
    
    # 释放原始数据内存
    del df_pandas, low_data_pandas, medium_data_pandas
    gc.collect()
    
    monitor.checkpoint('data_separated')
    
    print(f"✓ 数据分离完成")
    print(f"  low_* 数据: {low_data_polars.shape}")
    print(f"  medium_* 数据: {medium_data_polars.shape}")
    
    return low_data_polars, medium_data_polars


def shuffle_data_with_maximum_randomness(
    df_polars: pl.DataFrame, 
    random_seed: Optional[int] = None,
    monitor: PerformanceMonitor = None
) -> pl.DataFrame:
    """
    使用最大随机性算法打乱数据
    
    Args:
        df_polars: 需要打乱的Polars DataFrame
        random_seed: 随机种子（可选）
        monitor: 性能监控器
    
    Returns:
        pl.DataFrame: 打乱后的DataFrame
    """
    print(f"\n开始随机打乱数据...")
    
    # 设置随机种子（如果指定）
    if random_seed is not None:
        np.random.seed(random_seed)
        print(f"使用随机种子: {random_seed}")
    else:
        # 使用当前时间作为种子，确保高随机性
        seed = int(time.time() * 1000000) % (2**32)
        np.random.seed(seed)
        print(f"使用时间种子: {seed}")
    
    # 获取行数
    n_rows = df_polars.shape[0]
    
    # 方法1：使用Fisher-Yates洗牌算法生成随机索引
    print("使用Fisher-Yates洗牌算法生成随机索引...")
    indices = np.arange(n_rows)
    
    # Fisher-Yates洗牌 - 保证均匀分布
    for i in range(n_rows - 1, 0, -1):
        j = np.random.randint(0, i + 1)
        indices[i], indices[j] = indices[j], indices[i]
    
    # 额外随机化：多轮打乱增加随机性
    print("执行额外随机化...")
    for _ in range(3):  # 多轮打乱
        chunk_size = max(1000, n_rows // 100)  # 动态块大小
        for start in range(0, n_rows, chunk_size):
            end = min(start + chunk_size, n_rows)
            chunk_indices = indices[start:end]
            np.random.shuffle(chunk_indices)
            indices[start:end] = chunk_indices
    
    # 使用Polars的高效索引重排
    print("应用随机索引重排...")
    
    # 添加行索引列
    df_with_index = df_polars.with_row_count('original_index')
    
    # 创建随机索引映射
    shuffle_mapping = pl.DataFrame({
        'new_index': range(n_rows),
        'original_index': indices
    })
    
    # 执行连接和重排
    shuffled_df = (
        shuffle_mapping
        .join(df_with_index, on='original_index', how='left')
        .sort('new_index')
        .drop(['new_index', 'original_index'])
    )
    
    if monitor:
        monitor.checkpoint('data_shuffled')
    
    print(f"✓ 数据打乱完成: {shuffled_df.shape}")
    
    # 验证打乱效果
    print("验证打乱效果...")
    original_first_5 = df_polars.head(5).to_pandas()
    shuffled_first_5 = shuffled_df.head(5).to_pandas()
    
    # 计算前5行的相似度（应该很低）
    similarity_count = 0
    for i in range(min(5, n_rows)):
        if original_first_5.iloc[i].equals(shuffled_first_5.iloc[i]):
            similarity_count += 1
    
    print(f"前5行相似度: {similarity_count}/5 (越低越好)")
    
    return shuffled_df


def reconstruct_and_save_data(
    low_data: pl.DataFrame,
    shuffled_medium_data: pl.DataFrame,
    output_path: Path,
    monitor: PerformanceMonitor
) -> bool:
    """
    重构数据并保存到Excel
    
    Args:
        low_data: 前5列数据（未变）
        shuffled_medium_data: 打乱后的后5列数据
        output_path: 输出文件路径
        monitor: 性能监控器
    
    Returns:
        bool: 是否成功
    """
    try:
        print(f"\n重构并保存数据到: {output_path}")
        
        # 合并数据
        print("合并前5列和打乱后的后5列...")
        combined_polars = pl.concat([low_data, shuffled_medium_data], how='horizontal')
        
        monitor.checkpoint('data_combined')
        
        # 转换回pandas用于Excel保存
        print("转换为pandas DataFrame...")
        combined_pandas = combined_polars.to_pandas()
        
        # 释放Polars内存
        del combined_polars, low_data, shuffled_medium_data
        gc.collect()
        
        monitor.checkpoint('converted_to_pandas')
        
        # 确保输出目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 保存到Excel
        print("保存到Excel文件...")
        combined_pandas.to_excel(output_path, index=False, engine='openpyxl')
        
        monitor.checkpoint('data_saved')
        
        print(f"✓ 文件保存成功")
        print(f"最终数据形状: {combined_pandas.shape}")
        
        # 释放内存
        del combined_pandas
        gc.collect()
        
        return True
        
    except Exception as e:
        print(f"✗ 保存失败: {e}")
        return False


def shuffle_medium_columns(
    input_file: str,
    output_file: Optional[str] = None,
    random_seed: Optional[int] = None
) -> bool:
    """
    主处理函数：打乱medium列数据
    
    Args:
        input_file: 输入Excel文件路径
        output_file: 输出文件路径（可选）
        random_seed: 随机种子（可选）
    
    Returns:
        bool: 是否成功
    """
    print("=== 混合方案数据打乱工具 ===")
    
    # 初始化性能监控
    monitor = PerformanceMonitor()
    
    # 处理文件路径
    input_path = Path(input_file)
    if not input_path.exists():
        print(f"✗ 输入文件不存在: {input_path}")
        return False
    
    if output_file is None:
        output_path = input_path.parent / f"{input_path.stem}_shuffled.xlsx"
    else:
        output_path = Path(output_file)
    
    try:
        # 步骤1：加载和验证数据
        df_pandas, error = load_and_validate_data(input_path, monitor)
        if df_pandas is None:
            print(f"✗ {error}")
            return False
        
        # 步骤2：分离数据列
        low_data_polars, medium_data_polars = separate_data_columns(df_pandas, monitor)
        
        # 步骤3：随机打乱medium数据
        shuffled_medium_polars = shuffle_data_with_maximum_randomness(
            medium_data_polars, 
            random_seed=random_seed,
            monitor=monitor
        )
        
        # 释放原始medium数据内存
        del medium_data_polars
        gc.collect()
        
        # 步骤4：重构和保存数据
        success = reconstruct_and_save_data(
            low_data_polars,
            shuffled_medium_polars,
            output_path,
            monitor
        )
        
        if success:
            # 显示性能统计
            summary = monitor.get_summary()
            print(f"\n=== 处理统计 ===")
            print(f"总耗时: {summary['total_time']:.2f}秒")
            print(f"内存峰值: {summary['memory_peak_mb']:.1f}MB")
            print(f"输出文件: {output_path}")
            
            # 显示各阶段耗时
            print(f"\n各阶段耗时:")
            prev_time = 0
            for name, info in summary['checkpoints'].items():
                stage_time = info['time'] - prev_time
                print(f"  {name}: {stage_time:.2f}秒")
                prev_time = info['time']
        
        return success
        
    except Exception as e:
        print(f"✗ 处理失败: {e}")
        return False


def main():
    """主函数，支持命令行参数"""
    parser = argparse.ArgumentParser(
        description="混合方案数据打乱工具 - 使用Polars优化",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python %(prog)s final_merged.xlsx                    # 打乱medium列数据
  python %(prog)s final_merged.xlsx -o shuffled.xlsx  # 指定输出文件
  python %(prog)s final_merged.xlsx --seed 42         # 使用固定随机种子
        """
    )
    
    parser.add_argument('input_file', help='输入Excel文件路径（10列：前5列low_*，后5列medium_*）')
    parser.add_argument('-o', '--output', help='输出文件路径（默认：原文件名_shuffled.xlsx）')
    parser.add_argument('--seed', type=int, help='随机种子（用于可重现的结果）')
    
    args = parser.parse_args()
    
    success = shuffle_medium_columns(
        input_file=args.input_file,
        output_file=args.output,
        random_seed=args.seed
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()