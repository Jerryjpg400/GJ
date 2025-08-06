# GJ - 车辆数据处理系统
# GJ - Vehicle Data Processing System

一套遵循 Unix 设计哲学的高性能大数据处理工具集，专门用于车辆牌照数据的批量处理与分析。

A high-performance big data processing toolkit following Unix design philosophy, specifically designed for batch processing and analysis of vehicle license plate data.

## 🚀 项目概述 | Project Overview

本项目包含两个主要模块，用于处理大规模车辆数据：

This project contains two main modules for processing large-scale vehicle data:

1. **delete_table** - CSV 列删除与字符替换工具 | CSV Column Removal & Character Replacement Tool
2. **merge_table** - 车辆数据合并与筛选工具 | Vehicle Data Merging & Filtering Tool

## 📋 系统要求 | System Requirements

- Python 3.8+
- Conda 环境管理器 | Conda environment manager
- Unix/Linux/macOS 操作系统 | Unix/Linux/macOS operating system

## 🛠️ 安装 | Installation

### 1. 创建 Conda 环境 | Create Conda Environment

```bash
conda create -n gj python=3.8
conda activate gj
```

### 2. 安装依赖 | Install Dependencies

```bash
# delete_table 模块依赖 | delete_table module dependencies
cd delete_table
pip install -r requirements.txt

# merge_table 模块依赖 | merge_table module dependencies  
cd ../merge_table
pip install -r requirements.txt
```

## 📦 模块介绍 | Module Introduction

### 1. delete_table - CSV 处理模块 | CSV Processing Module

高性能并行 CSV 处理工具集，支持：

High-performance parallel CSV processing toolkit supporting:

- 🗑️ **列删除** | **Column Removal**: 批量删除 CSV 文件中的指定列
- 🔤 **字符替换** | **Character Replacement**: 替换文件内容和文件名中的特定字符
- ⚡ **并行处理** | **Parallel Processing**: 利用多核 CPU 加速处理
- 💾 **断点续传** | **Checkpoint Resume**: 支持中断后继续处理
- 📊 **进度监控** | **Progress Monitoring**: 实时查看处理进度

#### 主要工具 | Main Tools

- `csv_processor.py` - 核心 CSV 处理器 | Core CSV processor
- `parallel_processor.py` - 并行任务分发器 | Parallel task dispatcher
- `fast_parallel_processor.py` - 优化版并行处理器 | Optimized parallel processor
- `integrated_processor.py` - 集成处理器（推荐） | Integrated processor (recommended)
- `progress_monitor.py` - 进度监控工具 | Progress monitoring tool

#### 快速使用 | Quick Start

```bash
# 处理单个目录下的所有 CSV 文件
# Process all CSV files in a directory
python integrated_processor.py -i /path/to/data

# 自定义处理参数
# Custom processing parameters
python integrated_processor.py -i /path/to/data -c 1 -o 'ԥ' -n '豫' --mp-workers 8
```

### 2. merge_table - 车辆数据管理模块 | Vehicle Data Management Module

专门用于车辆牌照数据的管理和分析：

Specifically designed for vehicle license plate data management and analysis:

- 🚗 **车辆记录管理** | **Vehicle Record Management**: 批量处理车辆记录
- 📅 **日期筛选** | **Date Filtering**: 按日期范围筛选数据
- 📊 **Excel 处理** | **Excel Processing**: 支持 Excel 文件的读写和转换
- 🔀 **数据合并** | **Data Merging**: 合并多个数据源
- 📈 **里程数据分析** | **Mileage Data Analysis**: 分析车辆里程数据

#### 工作流程脚本 | Workflow Scripts

按执行顺序排列 | Listed in execution order:

1. `00_remove_vehicle_records.py` - 删除指定车辆记录 | Remove specific vehicle records
2. `01_batch_excel_processor.py` - 批量处理 Excel 文件 | Batch process Excel files
3. `02_filter_mileage_data.py` - 筛选里程数据 | Filter mileage data
4. `03_merge_low_medium.py` - 合并低中里程数据 | Merge low-medium mileage data
5. `04_concat_merged_files.py` - 连接合并文件 | Concatenate merged files
6. `05_concat_with_polars_sorted.py` - 使用 Polars 排序连接 | Concatenate with Polars sorting
7. `06_shuffle_medium_data.py` - 随机打乱中等数据 | Shuffle medium data

辅助工具 | Auxiliary Tools:
- `A1_generate_plate_dates_auto.py` - 自动生成车牌日期记录 | Auto-generate plate date records
- `fix_date_sorting.py` - 修复日期排序 | Fix date sorting
- `merge_to_excel.py` - 合并到 Excel | Merge to Excel

#### 数据目录结构 | Data Directory Structure

```
merge_table/
├── data/                    # Excel 数据文件 | Excel data files
│   ├── 20250101.xlsx
│   ├── 20250102.xlsx
│   └── ...
└── filter_data/            # 筛选条件文件 | Filter criteria files
    ├── delete_all.csv      # 需删除的所有车牌 | All plates to delete
    ├── from_2025_02_01.csv # 从特定日期开始的车牌 | Plates from specific date
    └── ...
```

## 🚀 快速开始 | Quick Start

### 示例 1：处理 CSV 文件 | Example 1: Process CSV Files

```bash
cd delete_table

# 删除所有 CSV 文件的第一列
# Remove first column from all CSV files
python fast_parallel_processor.py data/20231201

# 集成处理（删除列 + 字符替换）
# Integrated processing (column removal + character replacement)
python integrated_processor.py -i data/20231201
```

### 示例 2：处理车辆数据 | Example 2: Process Vehicle Data

```bash
cd merge_table

# 删除指定车辆记录
# Remove specific vehicle records
python 00_remove_vehicle_records.py -i data -d filter_data/delete_all.csv

# 批量处理 Excel 文件
# Batch process Excel files
python 01_batch_excel_processor.py data/
```

## 📊 性能指标 | Performance Metrics

- **CSV 处理速度** | **CSV Processing Speed**: ~2000-3000 文件/秒 (16核)
- **并行效率** | **Parallel Efficiency**: 支持自动检测 CPU 核心数
- **内存优化** | **Memory Optimization**: 流式处理，低内存占用

## 🔧 高级配置 | Advanced Configuration

### 环境变量 | Environment Variables

```bash
# 设置 Conda 环境路径
# Set Conda environment path
export CONDA_ENV_PATH=/Users/cccc/opt/anaconda3/envs/gj
```

### 并行处理参数 | Parallel Processing Parameters

- `--mp-workers`: 多进程工作线程数 | Number of multiprocessing workers
- `--thread-workers`: 线程工作数 | Number of thread workers
- `--checkpoint`: 检查点文件路径 | Checkpoint file path

## 📝 设计原则 | Design Principles

1. **简单性** | **Simplicity**: 每个工具只做一件事并做好
2. **可组合性** | **Composability**: 工具可以自由组合使用
3. **文本接口** | **Text Interface**: 使用标准输入输出
4. **模块化** | **Modularity**: 各组件独立，易于维护和扩展

## ⚠️ 注意事项 | Important Notes

1. 文件会被原地修改，建议先备份重要数据
2. 默认使用 UTF-8 编码，自动处理 BOM 标记
3. 支持断点续传功能（仅 parallel_processor.py）
4. 处理大量文件时建议使用 SSD 存储

## 🤝 贡献 | Contributing

欢迎提交 Issue 和 Pull Request！

Issues and Pull Requests are welcome!

## 📄 许可证 | License

本项目遵循 Unix 设计哲学，具体许可证待定。

This project follows Unix design philosophy. License TBD.

---

## 🏗️ 项目结构 | Project Structure

```
GJ/
├── delete_table/           # CSV 处理模块 | CSV processing module
│   ├── *.py               # Python 脚本 | Python scripts
│   ├── data/              # 数据目录 | Data directory
│   ├── requirements.txt   # 依赖文件 | Dependencies
│   ├── README.md          # 模块说明 | Module documentation
│   └── CLAUDE.md          # 开发配置 | Development configuration
│
├── merge_table/            # 车辆数据模块 | Vehicle data module
│   ├── *.py               # 处理脚本 | Processing scripts
│   ├── data/              # Excel 数据 | Excel data
│   ├── filter_data/       # 筛选数据 | Filter data
│   ├── requirements.txt   # 依赖文件 | Dependencies
│   └── CLAUDE.md          # 开发配置 | Development configuration
│
└── README.md              # 项目说明 | Project documentation
```

## 💡 技术栈 | Tech Stack

- **Python 3.8+**
- **pandas** - 数据处理 | Data processing
- **openpyxl** - Excel 文件处理 | Excel file handling
- **multiprocessing** - 并行计算 | Parallel computing
- **tqdm** - 进度条显示 | Progress bar display
- **psutil** - 系统资源监控 | System resource monitoring

## 🔍 更多信息 | More Information

- 开发者 | Developer: Big Data Science Expert
- 经验 | Experience: 10+ years in big data analysis
- 理念 | Philosophy: Unix Design Philosophy

---

*本项目持续更新中 | This project is continuously updated*

**Version**: 0.1  
**Last Updated**: 2025-02-06