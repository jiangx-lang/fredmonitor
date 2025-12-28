#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试新指标读取和解释"""

import sys
import io

# 强制设置标准输出为utf-8，解决Windows控制台乱码
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import sys
sys.path.append('.')

from crisis_monitor import compose_series, get_indicator_explanation

print("=" * 60)
print("测试新指标读取和解释")
print("=" * 60)

# 测试1: US_REAL_RATE_10Y
print("\n1. 测试 US_REAL_RATE_10Y 读取...")
ts1 = compose_series('US_REAL_RATE_10Y')
if ts1 is not None and not ts1.empty:
    print(f"   ✅ 成功读取，数据点数: {len(ts1)}")
    print(f"   最新日期: {ts1.index[-1]}")
    print(f"   最新值: {ts1.iloc[-1]:.4f}")
else:
    print("   ❌ 读取失败")

# 测试2: GOLD_REAL_RATE_DIFF
print("\n2. 测试 GOLD_REAL_RATE_DIFF 读取...")
ts2 = compose_series('GOLD_REAL_RATE_DIFF')
if ts2 is not None and not ts2.empty:
    print(f"   ✅ 成功读取，数据点数: {len(ts2)}")
    print(f"   最新日期: {ts2.index[-1]}")
    print(f"   最新值: {ts2.iloc[-1]:.4f}")
else:
    print("   ❌ 读取失败")

# 测试3: 指标解释
print("\n3. 测试指标解释...")
indicators = ['US_REAL_RATE_10Y', 'MTSDS133FMS', 'GOLD_REAL_RATE_DIFF']
for ind_id in indicators:
    explanation = get_indicator_explanation(ind_id)
    print(f"\n   {ind_id}:")
    print(f"   {explanation[:100]}...")

print("\n" + "=" * 60)
print("✅ 测试完成")
print("=" * 60)
