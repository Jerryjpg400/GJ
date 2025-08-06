#!/usr/bin/env python3
"""
立即修复final_merged.xlsx文件的日期排序问题
高性能处理大文件，支持进度显示和内存监控
"""

import pandas as pd
import time
from pathlib import Path
import psutil
import sys


def fix_date_sorting(file_path: str, date_column: str = 'low_日期', backup: bool = True):
    """
    修复Excel文件的日期排序问题
    
    Args:
        file_path: Excel文件路径
        date_column: 日期列名
        backup: 是否创建备份
    """
    
    file_path = Path(file_path)
    
    if not file_path.exists():
        print(f"❌ 文件不存在: {file_path}")
        return False
    
    print(f"🔧 开始修复文件: {file_path}")
    print(f"📅 目标列: {date_column}")
    
    # 内存监控
    process = psutil.Process()
    initial_memory = process.memory_info().rss / 1024 / 1024
    
    start_time = time.time()
    
    try:
        # 1. 创建备份
        if backup:
            backup_path = file_path.with_suffix('.backup.xlsx')
            print(f"💾 创建备份: {backup_path}")
            import shutil
            shutil.copy2(file_path, backup_path)
        
        # 2. 读取文件
        print(f"📖 读取文件...")
        df = pd.read_excel(file_path, engine='openpyxl')
        
        original_shape = df.shape
        print(f"✓ 读取完成，形状: {original_shape}")
        
        # 内存使用检查
        current_memory = process.memory_info().rss / 1024 / 1024
        print(f"🧠 内存使用: {current_memory:.1f}MB (+{current_memory - initial_memory:.1f}MB)")
        
        # 3. 检查日期列
        if date_column not in df.columns:
            print(f"❌ 列 '{date_column}' 不存在")
            available_date_cols = [col for col in df.columns if '日期' in col]
            print(f"📋 可用日期列: {available_date_cols}")
            return False
        
        # 4. 保存原始数据类型和格式
        original_dtype = df[date_column].dtype
        print(f"📋 原始日期格式: {original_dtype}")
        
        # 5. 创建临时日期列用于排序（不修改原列）
        print(f"🔄 创建排序用的临时日期列...")
        df['_temp_date_for_sorting'] = pd.to_datetime(df[date_column])
        
        # 6. 检查排序状态
        is_sorted = df['_temp_date_for_sorting'].is_monotonic_increasing
        if is_sorted:
            print(f"✅ 数据已经是升序排列，无需修复")
            df.drop('_temp_date_for_sorting', axis=1, inplace=True)
            return True
        
        # 7. 执行排序（使用临时列排序，但保持原始格式）
        print(f"📊 按 {date_column} 升序排序（保持原始格式）...")
        df_sorted = df.sort_values(by='_temp_date_for_sorting', ascending=True)
        
        # 8. 删除临时列，保持原始日期格式
        df_sorted.drop('_temp_date_for_sorting', axis=1, inplace=True)
        
        # 验证排序结果（重新创建临时日期列验证）
        temp_check = pd.to_datetime(df_sorted[date_column])
        assert temp_check.is_monotonic_increasing, "排序失败"
        
        # 9. 保存文件
        print(f"💾 保存排序后的文件...")
        df_sorted.to_excel(file_path, index=False, engine='openpyxl')
        
        # 10. 统计结果
        end_time = time.time()
        elapsed = end_time - start_time
        final_memory = process.memory_info().rss / 1024 / 1024
        
        print(f"\n{'='*50}")
        print(f"🎉 修复完成！")
        print(f"📊 统计信息:")
        print(f"  • 文件形状: {original_shape}")
        print(f"  • 排序列: {date_column}")
        print(f"  • 日期范围: {temp_check.min().date()} → {temp_check.max().date()}")
        print(f"  • 原始格式: {original_dtype} (已保持)")
        print(f"  • 处理时间: {elapsed:.2f} 秒")
        print(f"  • 峰值内存: {final_memory:.1f}MB")
        print(f"  • 备份文件: {backup_path if backup else '未创建'}")
        print(f"{'='*50}")
        
        return True
        
    except Exception as e:
        print(f"❌ 修复失败: {e}")
        return False


def main():
    """主函数"""
    file_path = "/Users/cccc/Desktop/GJ/merge_table/final_merged.xlsx"
    
    success = fix_date_sorting(file_path, date_column='low_日期', backup=True)
    
    if success:
        print(f"\n🎯 建议验证修复结果:")
        print(f"python analyze_date_sorting.py")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()