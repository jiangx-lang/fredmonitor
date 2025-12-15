#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试合成指标读取
"""

import pandas as pd
import os

def test_read_synthetic_indicators():
    """测试读取合成指标"""
    synthetic_indicators = [
        "CP_MINUS_DTB3",
        "SOFR20DMA_MINUS_DTB3", 
        "CORPDEBT_GDP_PCT",
        "RESERVES_ASSETS_PCT",
        "RESERVES_DEPOSITS_PCT"
    ]
    
    for sid in synthetic_indicators:
        csv_path = f"data/series/{sid}.csv"
        print(f"\n=== 测试 {sid} ===")
        
        if not os.path.exists(csv_path):
            print(f"❌ 文件不存在: {csv_path}")
            continue
            
        try:
            # 读取CSV文件
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
            print(f"📊 原始DataFrame形状: {df.shape}")
            print(f"📊 列名: {list(df.columns)}")
            print(f"📊 数据类型: {df.dtypes}")
            
            # 处理不同的列名格式
            if len(df.columns) == 1:
                ts = df.iloc[:, 0]  # 取第一列
                print(f"✅ 使用第一列数据")
            else:
                ts = df.squeeze()  # 尝试squeeze
                print(f"✅ 使用squeeze数据")
            
            print(f"📈 时间序列长度: {len(ts)}")
            print(f"📈 最新日期: {ts.index[-1]}")
            print(f"📈 最新值: {ts.iloc[-1]}")
            print(f"✅ {sid} 读取成功")
            
        except Exception as e:
            print(f"❌ {sid} 读取失败: {e}")

if __name__ == "__main__":
    print("测试合成指标读取...")
    test_read_synthetic_indicators()
    print("\n测试完成!")











