#!/usr/bin/env python3
"""
直接搜索真正的M1货币供应量数据
使用已知的M1系列ID
"""

import sys
import os
sys.path.append('.')

from scripts.fred_http import series_info
import json

def search_real_m1_direct():
    """直接搜索真正的M1数据"""
    
    print("🔍 直接搜索真正的M1货币供应量数据...")
    print("=" * 60)
    
    # 已知的M1系列ID
    known_m1_series = [
        'M1SL',      # M1 Money Stock
        'M1NS',      # M1 Money Stock (Not Seasonally Adjusted)
        'M1REAL',    # Real M1 Money Stock
        'CURRENCY',  # Currency Component of M1
        'CURRCIR',   # Currency in Circulation
        'DEMDEPSL',  # Demand Deposits
        'DEMDEPSN',  # Demand Deposits (Not Seasonally Adjusted)
        'SAVINGSL',  # Savings Deposits
        'TCDSL',     # Time Deposits
        'MMSL',      # Money Market Funds
        'VELOCITYM1', # M1 Velocity
        'VELOCITYM2'  # M2 Velocity
    ]
    
    found_series = []
    
    for series_id in known_m1_series:
        print(f"\n🔎 检查系列: {series_id}")
        try:
            result = series_info(series_id)
            series_list = result.get('seriess', [])
            
            if series_list:
                s = series_list[0]
                title = s.get('title', '')
                frequency = s.get('frequency', '')
                units = s.get('units', '')
                seasonal = s.get('seasonal_adjustment', '')
                
                print(f"  ✓ 找到: {title}")
                print(f"    频率: {frequency}")
                print(f"    单位: {units}")
                print(f"    季节性调整: {seasonal}")
                
                found_series.append({
                    'id': series_id,
                    'title': title,
                    'frequency': frequency,
                    'units': units,
                    'seasonal_adjustment': seasonal
                })
            else:
                print(f"  ❌ 未找到")
                
        except Exception as e:
            print(f"  ❌ 错误: {e}")
    
    print(f"\n📊 总共找到 {len(found_series)} 个真正的M1系列")
    
    if found_series:
        print(f"\n📋 真正的M1货币供应量系列:")
        for s in found_series:
            print(f"  {s['id']}: {s['title']} ({s['frequency']}, {s['units']})")
    
    return found_series

if __name__ == "__main__":
    search_real_m1_direct()
