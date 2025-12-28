#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证配置文件"""

import sys
import io

# 强制设置标准输出为utf-8，解决Windows控制台乱码
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import yaml
from pathlib import Path

config_path = Path(__file__).parent / "config" / "crisis_indicators.yaml"

with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

print("=" * 60)
print("验证配置文件")
print("=" * 60)
print(f"指标总数: {len(config['indicators'])}")

gold_indicators = [i for i in config['indicators'] if i.get('group') == '黄金见顶监控']
print(f"\n黄金见顶监控指标数: {len(gold_indicators)}")

for i, indicator in enumerate(gold_indicators, 1):
    print(f"\n{i}. {indicator['name']}")
    print(f"   ID: {indicator.get('id') or indicator.get('series_id')}")
    print(f"   分组: {indicator.get('group')}")
    print(f"   权重: {indicator.get('weight')}")
    print(f"   基准: {indicator.get('compare_to')}")
    print(f"   方向: {'高为险' if indicator.get('higher_is_risk') else '低为险'}")

print("\n" + "=" * 60)
