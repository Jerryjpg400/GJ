#!/usr/bin/env python3
"""
合并merged目录下的所有xlsx文件为单个文件
功能：
1. 自动发现并过滤有效的xlsx文件（排除临时文件）
2. 验证列结构一致性
3. 垂直合并所有数据，只保留单行列名
4. 支持大文件优化和进度显示
遵循Unix设计哲学：专注做好一件事
"""

import argparse
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd


def find_valid_xlsx_files(directory):
    """
    查找目录中有效的xlsx文件
    
    Args:
        directory: 目录路径
    
    Returns:
        list: 有效xlsx文件路径列表
    """
    directory = Path(directory)
    if not directory.exists():
        return []
    
    # 获取所有xlsx文件，排除临时文件和系统文件
    xlsx_files = []
    for file_path in directory.glob("*.xlsx"):
        # 排除临时文件和隐藏文件
        if not file_path.name.startswith(('~$', '.', '#')):
            xlsx_files.append(file_path)
    
    return sorted(xlsx_files)


def validate_column_consistency(file_paths):
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


def read_file_data(file_path):
    """
    读取单个文件的数据
    
    Args:
        file_path: 文件路径
    
    Returns:
        tuple: (文件名, DataFrame对象, 错误信息)
    """
    try:
        df = pd.read_excel(file_path)
        return file_path.name, df, None
    except Exception as e:
        return file_path.name, None, str(e)


def concat_xlsx_files(input_dir, output_file=None, use_parallel=True):
    """
    合并目录中的所有xlsx文件
    
    Args:
        input_dir: 输入目录路径
        output_file: 输出文件路径（可选）
        use_parallel: 是否使用并行读取
    
    Returns:
        bool: 是否成功
    """
    
    print(f"=== Excel文件合并工具 ===")
    
    # 1. 查找有效文件
    xlsx_files = find_valid_xlsx_files(input_dir)
    
    if not xlsx_files:
        print(f"在目录 {input_dir} 中未找到有效的xlsx文件")
        return False
    
    print(f"找到 {len(xlsx_files)} 个xlsx文件:")
    for file_path in xlsx_files:
        print(f"  - {file_path.name}")
    
    # 2. 验证列结构一致性
    print(f"\n验证列结构一致性...")
    is_consistent, columns, error_msg = validate_column_consistency(xlsx_files)
    
    if not is_consistent:
        print(f"✗ 列结构验证失败: {error_msg}")
        return False
    
    print(f"✓ 列结构一致，共 {len(columns)} 列")
    print(f"列名: {columns}")
    
    # 3. 读取并合并数据
    print(f"\n开始读取数据...")
    start_time = time.time()
    
    dataframes = []
    
    if use_parallel and len(xlsx_files) > 1:
        # 并行读取多个文件
        print(f"使用并行方式读取 {len(xlsx_files)} 个文件...")
        
        with ThreadPoolExecutor(max_workers=min(4, len(xlsx_files))) as executor:
            future_to_file = {
                executor.submit(read_file_data, file_path): file_path 
                for file_path in xlsx_files
            }
            
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                filename, df, error = future.result()
                
                if error:
                    print(f"✗ {filename}: 读取失败 - {error}")
                    return False
                else:
                    dataframes.append((filename, df))
                    print(f"✓ {filename}: {df.shape[0]} 行")
    else:
        # 串行读取
        for file_path in xlsx_files:
            filename, df, error = read_file_data(file_path)
            
            if error:
                print(f"✗ {filename}: 读取失败 - {error}")
                return False
            else:
                dataframes.append((filename, df))
                print(f"✓ {filename}: {df.shape[0]} 行")
    
    # 按文件名排序确保一致性
    dataframes.sort(key=lambda x: x[0])
    
    # 4. 合并数据
    print(f"\n合并数据...")
    try:
        # 提取DataFrame对象
        df_list = [df for _, df in dataframes]
        
        # 使用pd.concat垂直合并，ignore_index=True重置索引
        merged_df = pd.concat(df_list, ignore_index=True, sort=False)
        
        read_time = time.time() - start_time
        print(f"✓ 数据合并完成")
        print(f"最终形状: {merged_df.shape}")
        print(f"读取时间: {read_time:.2f}秒")
        
    except Exception as e:
        print(f"✗ 数据合并失败: {e}")
        return False
    
    # 5. 保存结果
    if output_file is None:
        output_file = Path(input_dir) / "merged_all.xlsx"
    else:
        output_file = Path(output_file)
    
    # 确保输出目录存在
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\n保存合并结果到: {output_file}")
    save_start = time.time()
    
    try:
        merged_df.to_excel(output_file, index=False, engine='openpyxl')
        save_time = time.time() - save_start
        
        print(f"✓ 文件保存成功")
        print(f"保存时间: {save_time:.2f}秒")
        
        # 统计信息
        total_time = time.time() - start_time
        print(f"\n=== 合并统计 ===")
        print(f"输入文件数: {len(xlsx_files)}")
        print(f"总行数: {merged_df.shape[0]}")
        print(f"总列数: {merged_df.shape[1]}")
        print(f"输出文件: {output_file.name}")
        print(f"总耗时: {total_time:.2f}秒")
        
        return True
        
    except Exception as e:
        print(f"✗ 文件保存失败: {e}")
        return False


def main():
    """主函数，支持命令行参数"""
    parser = argparse.ArgumentParser(
        description="合并目录中的所有xlsx文件为单个文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python %(prog)s merged/                           # 合并merged目录下所有xlsx文件
  python %(prog)s merged/ -o final_merged.xlsx     # 指定输出文件名
  python %(prog)s merged/ --no-parallel            # 禁用并行读取
        """
    )
    
    parser.add_argument('input_dir', help='包含xlsx文件的输入目录')
    parser.add_argument('-o', '--output', help='输出文件路径（默认：输入目录/merged_all.xlsx）')
    parser.add_argument('--no-parallel', action='store_true',
                       help='禁用并行读取（用于调试或低内存环境）')
    
    args = parser.parse_args()
    
    success = concat_xlsx_files(
        input_dir=args.input_dir,
        output_file=args.output,
        use_parallel=not args.no_parallel
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()