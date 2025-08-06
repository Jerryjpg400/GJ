#!/usr/bin/env python3
import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows

def merge_csv_to_excel():
    """
    将plate_dates.csv的数据合并到表格5.xlsx下面，创建新的表格5.1.xlsx
    """
    # 步骤1：读取表格5.xlsx
    print("步骤1：读取表格5.xlsx...")
    excel_path = '/Users/cccc/Desktop/GJ/merge_table/表格5.xlsx'
    try:
        # 首先尝试读取Excel文件以查看其结构
        excel_df = pd.read_excel(excel_path, engine='openpyxl')
        print(f"表格5.xlsx的行数：{len(excel_df)}")
        print(f"表格5.xlsx的列名：{list(excel_df.columns)}")
        print("\n表格5.xlsx的前5行预览：")
        print(excel_df.head().to_string(index=False))
    except Exception as e:
        print(f"读取表格5.xlsx时出错：{e}")
        # 如果文件不存在或为空，创建一个空的DataFrame
        excel_df = pd.DataFrame()
    
    # 步骤2：读取plate_dates.csv
    print("\n步骤2：读取plate_dates.csv...")
    csv_path = '/Users/cccc/Desktop/GJ/merge_table/plate_dates.csv'
    csv_df = pd.read_csv(csv_path)
    print(f"plate_dates.csv的行数：{len(csv_df)}")
    print(f"plate_dates.csv的列名：{list(csv_df.columns)}")
    
    # 步骤3：合并数据
    print("\n步骤3：合并数据...")
    if excel_df.empty:
        # 如果Excel文件为空，直接使用CSV数据
        merged_df = csv_df
    else:
        # 使用concat垂直合并两个DataFrame
        merged_df = pd.concat([excel_df, csv_df], ignore_index=True)
    
    print(f"合并后的总行数：{len(merged_df)}")
    
    # 步骤4：保存为新的Excel文件
    print("\n步骤4：保存为表格5.1.xlsx...")
    output_path = '/Users/cccc/Desktop/GJ/merge_table/表格5.1.xlsx'
    
    # 使用ExcelWriter保存
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        merged_df.to_excel(writer, index=False, sheet_name='Sheet1')
    
    print(f"文件已保存到：{output_path}")
    
    # 显示统计信息
    print("\n=== 统计信息 ===")
    print(f"原表格5.xlsx行数：{len(excel_df)}")
    print(f"plate_dates.csv行数：{len(csv_df)}")
    print(f"合并后总行数：{len(merged_df)}")
    
    # 显示合并后数据的预览
    if not excel_df.empty:
        # 显示原Excel数据的最后几行
        print("\n=== 原表格5.xlsx的最后5行 ===")
        print(excel_df.tail().to_string(index=False))
    
    # 显示CSV数据的前几行（这些会接在Excel数据后面）
    print("\n=== 新增的数据（前5行）===")
    print(csv_df.head().to_string(index=False))

if __name__ == "__main__":
    merge_csv_to_excel()