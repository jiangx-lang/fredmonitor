#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""临时脚本：验证黄金见顶判断模型数据文件"""

import sys
import io

# 强制设置标准输出为utf-8，解决Windows控制台乱码
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import pandas as pd
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent

files_to_check = [
    'data/series/US_REAL_RATE_10Y.csv',
    'data/series/GOLD_REAL_RATE_DIFF.csv'
]

print("=" * 60)
print("验证黄金见顶判断模型数据文件")
print("=" * 60)

for file_path in files_to_check:
    full_path = BASE_DIR / file_path
    print(f"\n{'='*60}")
    print(f"文件: {file_path}")
    print(f"存在: {full_path.exists()}")
    
    if full_path.exists():
        try:
            df = pd.read_csv(full_path, index_col=0, parse_dates=True)
            print(f"总数据点数: {len(df)}")
            print(f"\n最后5行数据:")
            print(df.tail(5))
            print(f"\n最新日期: {df.index[-1]}")
            print(f"最新值: {df.iloc[-1, 0]:.4f}")
        except Exception as e:
            print(f"读取失败: {e}")
    else:
        print("文件不存在！")

print("\n" + "=" * 60)
