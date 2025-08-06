#!/usr/bin/env python3
import pandas as pd
from datetime import datetime
import os
import re
from glob import glob

def generate_plate_dates_auto():
    """
    自动识别所有from_yyyy_mm_dd.csv格式的文件，
    根据文件名解析起始日期，生成车牌号日期记录
    """
    
    # 设置路径
    filter_data_dir = '/Users/cccc/Desktop/GJ/merge_table/filter_data'
    end_date = datetime(2025, 6, 30)
    
    # 步骤1：自动识别所有from_yyyy_mm_dd.csv格式的文件
    print("步骤1：自动识别所有from_yyyy_mm_dd.csv格式的文件...")
    
    # 使用glob查找所有匹配的文件
    pattern = os.path.join(filter_data_dir, 'from_*.csv')
    all_files = glob(pattern)
    
    # 用于匹配文件名格式的正则表达式
    filename_pattern = re.compile(r'from_(\d{4})_(\d{2})_(\d{2})\.csv')
    
    # 存储文件和解析的日期
    file_date_mapping = {}
    
    for file_path in all_files:
        filename = os.path.basename(file_path)
        match = filename_pattern.match(filename)
        
        if match:
            year, month, day = match.groups()
            start_date = datetime(int(year), int(month), int(day))
            file_date_mapping[filename] = start_date
            print(f"  发现文件：{filename} → 起始日期：{start_date.strftime('%Y/%m/%d')}")
    
    print(f"\n共发现 {len(file_date_mapping)} 个符合格式的文件")
    
    # 步骤2：读取每个文件的车牌号并生成日期记录
    print("\n步骤2：读取每个文件的车牌号并生成日期记录...\n")
    
    all_records = []
    total_plates = 0
    
    # 按文件名排序处理，确保结果的一致性
    for filename in sorted(file_date_mapping.keys()):
        start_date = file_date_mapping[filename]
        file_path = os.path.join(filter_data_dir, filename)
        
        print(f"处理文件：{filename}")
        print(f"起始日期：{start_date.strftime('%Y/%m/%d')}")
        
        # 读取CSV文件
        try:
            df = pd.read_csv(file_path)
            # 获取车牌号（第一列，跳过表头）
            license_plates = df.iloc[:, 0].dropna().tolist()
            
            # 如果第一个元素是"车牌号"（表头），则移除
            if license_plates and license_plates[0] == '车牌号':
                license_plates = license_plates[1:]
            
            print(f"车牌数量：{len(license_plates)}")
            
            # 生成日期范围
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            print(f"天数：{len(date_range)} 天（{start_date.strftime('%Y/%m/%d')} 到 {end_date.strftime('%Y/%m/%d')}）")
            print(f"车牌号列表：{', '.join(license_plates[:5])}{'...' if len(license_plates) > 5 else ''}")
            print(f"预计生成记录数：{len(license_plates)} × {len(date_range)} = {len(license_plates) * len(date_range)}")
            
            # 为每个车牌号生成记录
            for plate in license_plates:
                for date in date_range:
                    # 格式化日期为 YYYY/M/D（无前导零）
                    formatted_date = f"{date.year}/{date.month}/{date.day}"
                    all_records.append({
                        '车号': plate,
                        '日期': formatted_date
                    })
            
            total_plates += len(license_plates)
            
        except Exception as e:
            print(f"  处理文件时出错：{e}")
        
        print("-" * 70)
    
    # 步骤3：创建DataFrame
    print(f"\n步骤3：创建DataFrame...")
    result_df = pd.DataFrame(all_records)
    print(f"总记录数：{len(result_df)}")
    print(f"总车牌数：{total_plates}")
    
    # 步骤4：添加补充车辆和补充日期列
    print("\n步骤4：添加补充车辆和补充日期列...")
    result_df['补充车辆'] = '豫ND7570'
    result_df['补充日期'] = '2025/1/1'
    
    # 步骤5：保存文件
    print("\n步骤5：保存到plate_dates.csv...")
    output_path = '/Users/cccc/Desktop/GJ/merge_table/plate_dates.csv'
    result_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"文件已保存到：{output_path}")
    
    # 显示统计信息
    print("\n=== 统计信息 ===")
    print(f"处理的文件数：{len(file_date_mapping)}")
    print(f"总车牌数：{total_plates}")
    print(f"总记录数：{len(result_df)}")
    print(f"数据列：{list(result_df.columns)}")
    
    # 显示各车牌号的记录数统计
    print("\n=== 各车牌号记录数统计（前10个）===")
    plate_counts = result_df['车号'].value_counts()
    for i, (plate, count) in enumerate(plate_counts.items()):
        if i < 10:
            print(f"{plate}: {count}条记录")
        else:
            print(f"... 还有 {len(plate_counts) - 10} 个车牌号")
            break
    
    # 显示数据预览
    print("\n=== 数据预览（前10条）===")
    print(result_df.head(10).to_string(index=False))
    
    print("\n=== 数据预览（后10条）===")
    print(result_df.tail(10).to_string(index=False))
    
    return result_df

if __name__ == "__main__":
    generate_plate_dates_auto()