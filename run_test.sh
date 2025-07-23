#!/bin/bash
# 性能测试脚本

echo "CSV列删除性能测试"
echo "=================="
echo

# 设置变量
DIRECTORY="20231201"
WORKERS=16

# 测试fast_parallel_processor
echo "测试1: fast_parallel_processor.py"
echo "---------------------------------"
time python fast_parallel_processor.py $DIRECTORY $WORKERS
echo

# 清理并重新准备测试数据
# echo "准备下一个测试..."
# git checkout -- $DIRECTORY/*.csv 2>/dev/null || echo "无法恢复文件，继续测试"
# echo

# 测试parallel_processor
echo "测试2: parallel_processor.py"
echo "----------------------------"
time python parallel_processor.py $DIRECTORY $WORKERS checkpoint_test.json
echo

# 测试batch_processor
echo "测试3: batch_processor.py (完整版)"
echo "---------------------------------"
time python batch_processor.py $DIRECTORY -w $WORKERS -c checkpoint_batch.json
echo

echo "测试完成！"