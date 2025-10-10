#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""测试单个图表的中文显示"""
import pathlib
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 添加项目根目录到路径
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

from scripts.viz import save_indicator_plot

def test_single_chart():
    """测试单个图表"""
    print("🧪 测试单个图表中文显示...")
    
    # 创建测试数据
    dates = pd.date_range('2020-01-01', periods=50, freq='M')
    values = np.random.randn(50).cumsum() + 100
    
    ts = pd.Series(values, index=dates)
    
    # 模拟危机数据
    crises = [
        {'name': '测试危机', 'start': '2020-03-01', 'end': '2020-06-01'},
        {'name': '另一个危机', 'start': '2021-01-01', 'end': '2021-03-01'}
    ]
    
    crisis_stats = {
        'crisis_median': 95.0,
        'crisis_p25': 90.0,
        'crisis_p75': 105.0,
        'crisis_mean': 100.0,
        'crisis_std': 10.0
    }
    
    # 生成图表
    out_path = BASE / "test_chinese_chart.png"
    title = "密歇根消费者信心指数测试"
    unit = "指数"
    
    try:
        save_indicator_plot(
            ts=ts,
            title=title,
            unit=unit,
            crises=crises,
            crisis_stats=crisis_stats,
            out_path=out_path
        )
        print(f"✅ 图表已保存: {out_path}")
        
        # 检查文件大小
        file_size = out_path.stat().st_size
        print(f"📊 文件大小: {file_size:,} bytes")
        
    except Exception as e:
        print(f"❌ 图表生成失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_chart()



