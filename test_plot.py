#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试图表生成"""
import pathlib
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

# 添加项目根目录到路径
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

from scripts.crisis_monitor import (
    load_yaml_config, get_series_data, transform_series, calculate_crisis_stats
)
from scripts.viz import save_indicator_plot

def test_single_plot():
    """测试单个指标绘图"""
    series_id = "HOUST"
    name = "新屋开工"
    unit = "千套"
    
    print(f"测试 {name} ({series_id}) 图表生成...")
    
    # 获取数据
    crises = load_yaml_config(BASE / "config" / "crisis_periods.yaml")["crises"]
    s = get_series_data(series_id)
    if s is None or s.empty:
        print("❌ 无法获取数据")
        return False
        
    ts = transform_series(s, "level").dropna()
    if ts.empty:
        print("❌ 变换后无数据")
        return False
        
    cstats = calculate_crisis_stats(ts, crises)
    
    # 生成图表
    out_path = BASE / "test_plot.png"
    try:
        save_indicator_plot(ts, f"{name} ({series_id})", unit, crises, cstats, out_path)
        print(f"✅ 图表已保存: {out_path}")
        
        # 检查文件大小
        file_size = out_path.stat().st_size
        print(f"📊 文件大小: {file_size:,} bytes")
        
        if file_size > 10000:  # 至少10KB
            print("✅ 图表文件大小正常")
            return True
        else:
            print("❌ 图表文件过小，可能损坏")
            return False
            
    except Exception as e:
        print(f"❌ 图表生成失败: {e}")
        return False

if __name__ == "__main__":
    success = test_single_plot()
    if success:
        print("\n🎉 图表生成测试成功！")
    else:
        print("\n❌ 图表生成测试失败！")


