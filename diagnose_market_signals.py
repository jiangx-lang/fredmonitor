#!/usr/bin/env python3
"""
诊断市场信号文件读取问题
"""

import sys
import pathlib
import pandas as pd

# 强制设置标准输出为utf-8
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

BASE = pathlib.Path(__file__).parent

print("=" * 60)
print("🔍 诊断市场信号文件读取问题")
print("=" * 60)

# 检查文件是否存在
signals = ['MKT_SPY_TREND_STATUS', 'MKT_SPY_REALIZED_VOL', 'MKT_CREDIT_APPETITE']
for sig in signals:
    csv_path = BASE / "data" / "series" / f"{sig}.csv"
    print(f"\n📄 {sig}:")
    print(f"   路径: {csv_path}")
    print(f"   存在: {csv_path.exists()}")
    
    if csv_path.exists():
        try:
            # 尝试读取（不带index_col，看格式）
            df_raw = pd.read_csv(csv_path)
            print(f"   原始格式 - 列: {df_raw.columns.tolist()}, 形状: {df_raw.shape}")
            print(f"   前3行:")
            print(df_raw.head(3))
            print(f"   后3行:")
            print(df_raw.tail(3))
            
            # 尝试按 compose_series 的方式读取（index_col=0）
            try:
                df_indexed = pd.read_csv(csv_path, index_col=0, parse_dates=True)
                print(f"   ✅ 索引格式读取成功 - 列: {df_indexed.columns.tolist()}, 形状: {df_indexed.shape}")
                if 'value' in df_indexed.columns:
                    print(f"   最新值: {df_indexed['value'].iloc[-1]}")
                elif len(df_indexed.columns) == 1:
                    print(f"   最新值: {df_indexed.iloc[-1, 0]}")
            except Exception as e:
                print(f"   ❌ 索引格式读取失败: {e}")
            
            # 尝试按 load_market_indicator 的方式读取（不带index_col）
            try:
                df_no_index = pd.read_csv(csv_path)
                print(f"   ✅ 无索引格式读取成功 - 列: {df_no_index.columns.tolist()}, 形状: {df_no_index.shape}")
                if 'date' in df_no_index.columns and 'value' in df_no_index.columns:
                    print(f"   最新值: {df_no_index['value'].iloc[-1]}")
                elif 'value' in df_no_index.columns:
                    print(f"   最新值: {df_no_index['value'].iloc[-1]}")
            except Exception as e:
                print(f"   ❌ 无索引格式读取失败: {e}")
                
        except Exception as e:
            print(f"   ❌ 读取失败: {e}")

print("\n" + "=" * 60)
print("诊断完成")
print("=" * 60)
