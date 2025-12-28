#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""搜索FRED中的黄金价格序列"""

import sys
import io

# 强制设置标准输出为utf-8，解决Windows控制台乱码
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import os
import sys
import pathlib

BASE = pathlib.Path(__file__).parent
sys.path.append(str(BASE))

from scripts.fred_http import series_search, series_info

print("=" * 60)
print("搜索FRED中的黄金价格序列")
print("=" * 60)

# 搜索关键词
search_terms = [
    "gold price",
    "gold fixing",
    "gold london",
    "GOLDAMGBD",
    "gold",
    "precious metal"
]

all_gold_series = []

for term in search_terms:
    print(f"\n🔍 搜索关键词: {term}")
    try:
        result = series_search(term, limit=20)
        series_list = result.get('seriess', [])
        
        print(f"   找到 {len(series_list)} 个序列")
        
        for s in series_list[:10]:  # 显示前10个
            series_id = s.get('id', '')
            title = s.get('title', '')
            frequency = s.get('frequency', '')
            units = s.get('units', '')
            print(f"   {series_id}: {title}")
            print(f"      频率: {frequency}, 单位: {units}")
        
        all_gold_series.extend(series_list)
        
    except Exception as e:
        print(f"   ❌ 搜索失败: {e}")

# 去重
unique_series = {}
for s in all_gold_series:
    series_id = s.get('id', '')
    if series_id not in unique_series:
        unique_series[series_id] = s

print("\n" + "=" * 60)
print(f"📊 总共找到 {len(unique_series)} 个唯一的黄金相关序列")
print("=" * 60)

# 显示所有找到的序列
print("\n📋 所有黄金相关序列:")
for i, (series_id, s) in enumerate(list(unique_series.items())[:30], 1):  # 显示前30个
    title = s.get('title', '')
    frequency = s.get('frequency', '')
    units = s.get('units', '')
    start_date = s.get('observation_start', '')
    end_date = s.get('observation_end', '')
    print(f"\n{i}. {series_id}")
    print(f"   标题: {title}")
    print(f"   频率: {frequency}, 单位: {units}")
    print(f"   日期范围: {start_date} 至 {end_date}")

# 测试几个可能的序列ID
print("\n" + "=" * 60)
print("测试可能的序列ID")
print("=" * 60)

test_ids = [
    "GOLDAMGBD228NLBM",  # 原始ID
    "GOLDAMGBD228NLBM",  # 可能的大小写问题
    "GOLD",  # 简化ID
    "GOLDPRICE",  # 可能的别名
    "GOLDAMGBD",  # 部分ID
]

for test_id in test_ids:
    print(f"\n测试序列ID: {test_id}")
    try:
        meta_response = series_info(test_id)
        series_list = meta_response.get("seriess", [])
        if series_list:
            meta = series_list[0]
            print(f"  ✅ 找到序列: {meta.get('title', 'N/A')}")
            print(f"     开始日期: {meta.get('observation_start', 'N/A')}")
            print(f"     结束日期: {meta.get('observation_end', 'N/A')}")
        else:
            print(f"  ❌ 未找到序列")
    except Exception as e:
        print(f"  ❌ 错误: {str(e)[:100]}")

print("\n" + "=" * 60)
