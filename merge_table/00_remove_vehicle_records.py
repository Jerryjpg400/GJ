#!/usr/bin/env python3
"""
高性能并行化车牌记录删除工具
支持批量删除XLSX文件中的车牌记录，具备智能负载均衡和实时监控功能
"""

import os
import sys
import argparse
import pandas as pd
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
import gc
import psutil
from typing import List, Set, Tuple, Optional, Dict
import time
from dataclasses import dataclass
from functools import partial
import logging


@dataclass
class ProcessingStats:
    """处理统计信息"""
    total_files: int = 0
    processed_files: int = 0
    failed_files: int = 0
    total_records_deleted: int = 0
    start_time: float = 0
    
    @property
    def success_rate(self) -> float:
        if self.total_files == 0:
            return 0.0
        return (self.processed_files / self.total_files) * 100
    
    @property
    def processing_speed(self) -> float:
        elapsed = time.time() - self.start_time
        if elapsed == 0:
            return 0.0
        return self.processed_files / elapsed


class VehicleDataProcessor:
    """核心数据处理模块"""
    
    def __init__(self, vehicle_plates: Set[str], from_date: Optional[datetime] = None):
        self.vehicle_plates = vehicle_plates
        self.from_date = from_date
        
    def extract_date_from_filename(self, filename: str) -> Optional[datetime]:
        """从文件名提取日期 (e.g., '0101.xlsx' -> datetime(2025, 1, 1))"""
        try:
            stem = Path(filename).stem
            if len(stem) == 4 and stem.isdigit():
                month = int(stem[:2])
                day = int(stem[2:])
                # 默认使用当前年份
                year = datetime.now().year
                return datetime(year, month, day)
        except (ValueError, IndexError):
            pass
        return None
    
    def extract_date_from_content(self, df: pd.DataFrame) -> Optional[datetime]:
        """从数据内容中提取日期"""
        for col in df.columns:
            if any(keyword in str(col).lower() for keyword in ['时间', 'date', '日期']):
                try:
                    # 查找特殊格式: "2025/1/1 0:00:00---2025/1/1 0:00:00合计:"
                    for value in df[col].dropna().head(10):
                        value_str = str(value)
                        if '---' in value_str and '合计' in value_str:
                            date_part = value_str.split('---')[0].strip()
                            return datetime.strptime(date_part.split()[0], '%Y/%m/%d')
                except (ValueError, AttributeError):
                    continue
        return None
    
    def should_process_file(self, file_path: str) -> bool:
        """判断是否应该处理该文件（基于日期过滤）"""
        if not self.from_date:
            return True
            
        # 先尝试从文件名提取日期
        file_date = self.extract_date_from_filename(file_path)
        if file_date:
            return file_date >= self.from_date
            
        # 如果文件名无法提取日期，则需要打开文件检查内容
        return True  # 延迟到内容检查时判断
    
    def process_single_file(self, file_path: str) -> Tuple[bool, int, str]:
        """处理单个XLSX文件"""
        try:
            # 读取Excel文件
            df = pd.read_excel(file_path, engine='openpyxl')
            
            if df.empty:
                return True, 0, "空文件"
            
            # 如果有日期过滤且文件名无法确定日期，检查文件内容
            if self.from_date and not self.extract_date_from_filename(file_path):
                content_date = self.extract_date_from_content(df)
                if content_date and content_date < self.from_date:
                    return True, 0, "日期不符合过滤条件"
            
            # 查找车牌号列
            plate_column = None
            for col in df.columns:
                if any(keyword in str(col).lower() for keyword in ['车牌', 'plate', '号牌']):
                    plate_column = col
                    break
            
            if plate_column is None:
                return False, 0, "未找到车牌号列"
            
            # 删除匹配的车牌记录
            original_count = len(df)
            df_filtered = df[~df[plate_column].astype(str).isin(self.vehicle_plates)]
            deleted_count = original_count - len(df_filtered)
            
            if deleted_count > 0:
                # 保存修改后的文件
                df_filtered.to_excel(file_path, index=False, engine='openpyxl')
            
            return True, deleted_count, f"成功删除 {deleted_count} 条记录"
            
        except Exception as e:
            return False, 0, f"处理失败: {str(e)}"


def process_file_batch(file_paths: List[str], vehicle_plates: Set[str], 
                      from_date: Optional[datetime] = None) -> Dict:
    """批量处理文件（子进程执行）"""
    processor = VehicleDataProcessor(vehicle_plates, from_date)
    results = {
        'processed': 0,
        'failed': 0,
        'total_deleted': 0,
        'errors': []
    }
    
    for file_path in file_paths:
        if not processor.should_process_file(file_path):
            continue
            
        success, deleted_count, message = processor.process_single_file(file_path)
        
        if success:
            results['processed'] += 1
            results['total_deleted'] += deleted_count
        else:
            results['failed'] += 1
            results['errors'].append(f"{file_path}: {message}")
    
    return results


class ParallelVehicleRemover:
    """并行化车牌记录删除器"""
    
    def __init__(self, max_workers: Optional[int] = None, memory_limit_gb: float = 2.0):
        self.max_workers = max_workers or min(mp.cpu_count(), 8)
        self.memory_limit_bytes = memory_limit_gb * 1024 * 1024 * 1024
        self.stats = ProcessingStats()
        
    def _get_memory_usage(self) -> float:
        """获取当前内存使用率"""
        process = psutil.Process()
        return process.memory_info().rss
    
    def _balance_workload(self, xlsx_files: List[str]) -> List[List[str]]:
        """智能负载均衡 - 根据文件大小分配任务"""
        if not xlsx_files:
            return []
            
        # 获取文件大小
        file_weights = []
        for file_path in xlsx_files:
            try:
                size = os.path.getsize(file_path)
                file_weights.append((file_path, size))
            except OSError:
                # 文件不存在或无法访问，跳过
                continue
        
        if not file_weights:
            return []
        
        # 按文件大小降序排列
        file_weights.sort(key=lambda x: x[1], reverse=True)
        
        # 贪心算法分配到各个worker
        workers = [[] for _ in range(self.max_workers)]
        worker_loads = [0] * self.max_workers
        
        for file_path, size in file_weights:
            # 分配给当前负载最小的worker
            min_idx = worker_loads.index(min(worker_loads))
            workers[min_idx].append(file_path)
            worker_loads[min_idx] += size
        
        # 过滤掉空的worker组
        return [worker for worker in workers if worker]
    
    def _monitor_progress(self, futures: List, total_files: int) -> ProcessingStats:
        """实时监控处理进度"""
        self.stats.total_files = total_files
        self.stats.start_time = time.time()
        
        print(f"🚀 并行处理开始 [Workers: {len(futures)}, Files: {total_files}]")
        
        for future in as_completed(futures):
            try:
                result = future.result()
                self.stats.processed_files += result['processed']
                self.stats.failed_files += result['failed']
                self.stats.total_records_deleted += result['total_deleted']
                
                # 输出进度
                progress = (self.stats.processed_files + self.stats.failed_files) / total_files
                memory_mb = self._get_memory_usage() / (1024 * 1024)
                
                print(f"\r📊 Progress: {'█' * int(progress * 20):<20} "
                      f"{progress * 100:.1f}% ({self.stats.processed_files + self.stats.failed_files}/{total_files}) "
                      f"⚡ {self.stats.processing_speed:.1f} files/sec "
                      f"🧠 {memory_mb:.0f}MB", end='', flush=True)
                
                # 处理错误信息
                if result['errors']:
                    for error in result['errors']:
                        logging.warning(error)
                        
            except Exception as e:
                logging.error(f"处理任务时发生错误: {e}")
                self.stats.failed_files += 1
        
        print()  # 换行
        return self.stats
    
    def process_files_parallel(self, xlsx_files: List[str], vehicle_plates: Set[str], 
                             from_date: Optional[datetime] = None) -> ProcessingStats:
        """并行处理所有文件"""
        if not xlsx_files:
            print("❌ 没有找到需要处理的XLSX文件")
            return self.stats
        
        # 负载均衡分组
        file_groups = self._balance_workload(xlsx_files)
        
        if not file_groups:
            print("❌ 所有文件都无法访问")
            return self.stats
        
        # 创建进程池并提交任务
        with ProcessPoolExecutor(max_workers=len(file_groups)) as executor:
            process_func = partial(process_file_batch, 
                                 vehicle_plates=vehicle_plates, 
                                 from_date=from_date)
            
            futures = [executor.submit(process_func, file_group) 
                      for file_group in file_groups]
            
            # 监控进度
            self.stats = self._monitor_progress(futures, len(xlsx_files))
        
        return self.stats


def load_vehicle_plates(csv_file: str) -> Set[str]:
    """从CSV文件加载车牌号列表"""
    try:
        df = pd.read_csv(csv_file)
        
        # 查找车牌号列
        plate_column = None
        for col in df.columns:
            if any(keyword in str(col).lower() for keyword in ['车牌', 'plate', '号牌']):
                plate_column = col
                break
        
        if plate_column is None:
            # 如果没有明确的车牌列，使用第一列
            plate_column = df.columns[0]
            print(f"⚠️  未找到明确的车牌列，使用第一列: {plate_column}")
        
        plates = set(df[plate_column].astype(str).str.strip())
        plates.discard('')  # 移除空字符串
        plates.discard('nan')  # 移除NaN值
        
        print(f"📋 成功加载 {len(plates)} 个车牌号")
        return plates
        
    except Exception as e:
        print(f"❌ 加载车牌号文件失败: {e}")
        sys.exit(1)


def find_xlsx_files(data_dir: str) -> List[str]:
    """查找所有XLSX文件"""
    data_path = Path(data_dir)
    if not data_path.exists():
        print(f"❌ 数据目录不存在: {data_dir}")
        return []
    
    xlsx_files = list(data_path.glob("*.xlsx"))
    print(f"📁 在目录 {data_dir} 中找到 {len(xlsx_files)} 个XLSX文件")
    
    return [str(f) for f in xlsx_files]


def parse_date(date_str: str) -> datetime:
    """解析日期字符串"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print(f"❌ 日期格式错误: {date_str}，请使用 YYYY-MM-DD 格式")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="高性能并行化车牌记录删除工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python remove_vehicle_records.py filter_data/delete_all.csv
  python remove_vehicle_records.py filter_data/from_2025_02_01.csv --from-date 2025-02-01
  python remove_vehicle_records.py filter_data/delete_all.csv --parallel --workers 6
        """
    )
    
    parser.add_argument('csv_file', help='包含要删除车牌号的CSV文件')
    parser.add_argument('--from-date', type=parse_date, 
                       help='只删除指定日期及以后的记录 (格式: YYYY-MM-DD)')
    parser.add_argument('--data-dir', default='data', 
                       help='XLSX文件所在目录 (默认: data)')
    parser.add_argument('--parallel', action='store_true', 
                       help='启用并行处理')
    parser.add_argument('--workers', type=int, 
                       help='并行worker数量 (默认: 自动检测)')
    parser.add_argument('--memory-limit', type=float, default=2.0,
                       help='内存限制 (GB, 默认: 2.0)')
    parser.add_argument('--verbose', action='store_true',
                       help='详细输出')
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
    
    # 加载车牌号
    vehicle_plates = load_vehicle_plates(args.csv_file)
    if not vehicle_plates:
        print("❌ 没有加载到任何车牌号")
        return
    
    # 查找XLSX文件
    xlsx_files = find_xlsx_files(args.data_dir)
    if not xlsx_files:
        return
    
    # 开始处理
    start_time = time.time()
    
    if args.parallel:
        # 并行处理
        remover = ParallelVehicleRemover(
            max_workers=args.workers,
            memory_limit_gb=args.memory_limit
        )
        stats = remover.process_files_parallel(xlsx_files, vehicle_plates, args.from_date)
    else:
        # 串行处理
        processor = VehicleDataProcessor(vehicle_plates, args.from_date)
        stats = ProcessingStats()
        stats.total_files = len(xlsx_files)
        stats.start_time = start_time
        
        for i, file_path in enumerate(xlsx_files, 1):
            if processor.should_process_file(file_path):
                success, deleted_count, message = processor.process_single_file(file_path)
                if success:
                    stats.processed_files += 1
                    stats.total_records_deleted += deleted_count
                else:
                    stats.failed_files += 1
                    logging.warning(f"{file_path}: {message}")
            
            # 简单进度显示
            if i % 10 == 0 or i == len(xlsx_files):
                print(f"处理进度: {i}/{len(xlsx_files)}")
    
    # 输出统计结果
    elapsed_time = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"🎯 处理完成！")
    print(f"📊 统计信息:")
    print(f"   • 总文件数: {stats.total_files}")
    print(f"   • 成功处理: {stats.processed_files}")
    print(f"   • 处理失败: {stats.failed_files}")
    print(f"   • 成功率: {stats.success_rate:.1f}%")
    print(f"   • 删除记录总数: {stats.total_records_deleted}")
    print(f"   • 处理耗时: {elapsed_time:.2f} 秒")
    print(f"   • 平均速度: {stats.processing_speed:.2f} 文件/秒")
    print(f"{'='*50}")


if __name__ == '__main__':
    main()