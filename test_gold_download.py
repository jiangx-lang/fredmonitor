#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""单独测试下载 GOLDAMGBD228NLBM"""

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

# 添加项目路径
BASE = pathlib.Path(__file__).parent
sys.path.append(str(BASE))

from scripts.fred_http import series_info, series_observations, polite_sleep

SERIES_ID = "GOLDAMGBD228NLBM"
SERIES_ROOT = BASE / "data" / "fred" / "series"

print("=" * 60)
print(f"测试下载序列: {SERIES_ID}")
print("=" * 60)

# 测试1: 获取序列信息
print("\n1. 测试获取序列元数据...")
try:
    meta_response = series_info(SERIES_ID)
    series_list = meta_response.get("seriess", [])
    
    if series_list:
        meta = series_list[0]
        print(f"✅ 序列信息获取成功")
        print(f"   标题: {meta.get('title', 'N/A')}")
        print(f"   单位: {meta.get('units', 'N/A')}")
        print(f"   频率: {meta.get('frequency', 'N/A')}")
        print(f"   开始日期: {meta.get('observation_start', 'N/A')}")
        print(f"   结束日期: {meta.get('observation_end', 'N/A')}")
    else:
        print(f"❌ 未找到序列信息")
        sys.exit(1)
except Exception as e:
    print(f"❌ 获取序列信息失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 测试2: 获取观测数据（少量数据测试）
print("\n2. 测试获取观测数据（最近10条）...")
try:
    # 先尝试获取最近的数据
    obs_response = series_observations(SERIES_ID, limit=10)
    observations = obs_response.get("observations", [])
    
    if observations:
        print(f"✅ 成功获取 {len(observations)} 条观测数据")
        print(f"\n最近5条数据:")
        for obs in observations[-5:]:
            print(f"   {obs.get('date')}: {obs.get('value')}")
    else:
        print(f"❌ 无观测数据")
        sys.exit(1)
except Exception as e:
    print(f"❌ 获取观测数据失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# 测试3: 获取完整历史数据
print("\n3. 测试获取完整历史数据...")
try:
    obs_response = series_observations(SERIES_ID)
    observations = obs_response.get("observations", [])
    
    if observations:
        print(f"✅ 成功获取 {len(observations)} 条完整数据")
        
        # 转换为DataFrame
        df = pd.DataFrame(observations)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df["date"] = pd.to_datetime(df["date"])
        df = df[["date", "value"]].dropna()
        
        print(f"✅ 有效数据点: {len(df)}")
        print(f"   最早日期: {df['date'].min()}")
        print(f"   最新日期: {df['date'].max()}")
        print(f"   最新值: {df.iloc[-1]['value']:.2f}")
        
        # 保存测试文件
        test_file = BASE / "data" / "fred" / "series" / SERIES_ID / "raw.csv"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(test_file, index=False, encoding='utf-8')
        print(f"✅ 测试文件已保存: {test_file}")
        
    else:
        print(f"❌ 无完整数据")
        sys.exit(1)
except Exception as e:
    print(f"❌ 获取完整数据失败: {e}")
    import traceback
    traceback.print_exc()
    
    # 如果失败，尝试其他可能的序列ID
    print("\n" + "=" * 60)
    print("尝试查找替代序列...")
    print("=" * 60)
    
    alternative_ids = [
        "GOLDAMGBD228NLBM",  # 原始ID
        "GOLD",  # 简化ID
        "GOLDPRICE",  # 可能的别名
    ]
    
    for alt_id in alternative_ids:
        if alt_id == SERIES_ID:
            continue
        print(f"\n尝试序列: {alt_id}")
        try:
            meta_response = series_info(alt_id)
            series_list = meta_response.get("seriess", [])
            if series_list:
                meta = series_list[0]
                print(f"  ✅ 找到序列: {meta.get('title', 'N/A')}")
                print(f"     开始日期: {meta.get('observation_start', 'N/A')}")
                print(f"     结束日期: {meta.get('observation_end', 'N/A')}")
        except:
            print(f"  ❌ 未找到")
    
    sys.exit(1)

print("\n" + "=" * 60)
print("✅ 测试完成！序列可以正常下载")
print("=" * 60)
