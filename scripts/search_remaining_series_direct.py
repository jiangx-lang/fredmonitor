#!/usr/bin/env python3
"""
直接搜索剩余的Monetary Data系列
使用已知的系列ID
"""

import sys
import os
sys.path.append('.')

from scripts.fred_http import series_info
import json

def search_remaining_series_direct():
    """直接搜索剩余的Monetary Data系列"""
    
    print("🔍 直接搜索剩余的Monetary Data系列...")
    print("=" * 60)
    
    # 已知的剩余系列ID
    known_series = {
        'M2_Minus_Small_Time_Deposits': [
            'M2MNS', 'M2MSL'  # 这些已经在M2_Components中了
        ],
        'Money_Velocity': [
            'VELOCITYM1', 'VELOCITYM2', 'VELOCITYM3'
        ],
        'Borrowings': [
            'BORROWINGS', 'BORROWED', 'BORROWINGS'
        ],
        'Memorandum_Items': [
            'MEMO', 'MEMORANDUM'
        ],
        'Factors_Affecting_Reserve_Balances': [
            'FARB', 'FACTORS', 'RESERVE_FACTORS'
        ],
        'Securities_Loans_Assets_Liabilities': [
            'SECURITIES', 'LOANS', 'ASSETS', 'LIABILITIES'
        ]
    }
    
    found_series = {}
    
    for subcategory, series_ids in known_series.items():
        print(f"\n🔎 检查 {subcategory} 系列...")
        found_series[subcategory] = []
        
        for series_id in series_ids:
            try:
                result = series_info(series_id)
                series_list = result.get('seriess', [])
                
                if series_list:
                    s = series_list[0]
                    title = s.get('title', '')
                    frequency = s.get('frequency', '')
                    units = s.get('units', '')
                    
                    print(f"  ✓ {series_id}: {title}")
                    print(f"    频率: {frequency}, 单位: {units}")
                    
                    found_series[subcategory].append({
                        'id': series_id,
                        'title': title,
                        'frequency': frequency,
                        'units': units,
                        'seasonal_adjustment': s.get('seasonal_adjustment', '')
                    })
                else:
                    print(f"  ❌ {series_id}: 未找到")
                    
            except Exception as e:
                print(f"  ❌ {series_id}: 错误 - {e}")
    
    # 统计结果
    print(f"\n📊 搜索结果统计:")
    for subcategory, series_list in found_series.items():
        print(f"  {subcategory}: {len(series_list)} 个系列")
        if series_list:
            for s in series_list:
                print(f"    {s['id']}: {s['title']}")
    
    return found_series

if __name__ == "__main__":
    search_remaining_series_direct()
