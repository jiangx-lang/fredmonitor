#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试替代的黄金价格序列"""

import sys
import io

# 强制设置标准输出为utf-8，解决Windows控制台乱码
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import os
import sys
import pathlib
import pandas as pd

BASE = pathlib.Path(__file__).parent
sys.path.append(str(BASE))

from scripts.fred_http import series_info, series_observations

# 可能的替代序列ID
alternative_ids = [
    "NASDAQQGLDI",  # Credit Suisse NASDAQ Gold FLOWS103 Price Index (日频)
    "GVZCLS",  # CBOE Gold ETF Volatility Index (日频，但这是波动率，不是价格)
    "IQ12260",  # Export Price Index (End Use): Nonmonetary Gold (月频)
    "IR14270",  # Import Price Index (End Use): Nonmonetary Gold (月频)
]

print("=" * 60)
print("测试替代的黄金价格序列")
print("=" * 60)

for series_id in alternative_ids:
    print(f"\n{'='*60}")
    print(f"测试序列: {series_id}")
    print("=" * 60)
    
    try:
        # 获取序列信息
        meta_response = series_info(series_id)
        series_list = meta_response.get("seriess", [])
        
        if series_list:
            meta = series_list[0]
            print(f"✅ 序列存在")
            print(f"   标题: {meta.get('title', 'N/A')}")
            print(f"   单位: {meta.get('units', 'N/A')}")
            print(f"   频率: {meta.get('frequency', 'N/A')}")
            print(f"   开始日期: {meta.get('observation_start', 'N/A')}")
            print(f"   结束日期: {meta.get('observation_end', 'N/A')}")
            
            # 获取最近的数据
            obs_response = series_observations(series_id, limit=5)
            observations = obs_response.get("observations", [])
            
            if observations:
                print(f"\n   最近5条数据:")
                for obs in observations:
                    print(f"     {obs.get('date')}: {obs.get('value')}")
        else:
            print(f"❌ 序列不存在")
            
    except Exception as e:
        print(f"❌ 测试失败: {e}")

print("\n" + "=" * 60)
print("建议:")
print("=" * 60)
print("如果FRED中没有合适的黄金价格序列，可以考虑：")
print("1. 使用第三方API（如Alpha Vantage, Yahoo Finance等）")
print("2. 使用CSV文件手动导入")
print("3. 使用其他数据源（如World Gold Council）")
print("=" * 60)
