#!/usr/bin/env python3
"""
搜索真正的M1货币供应量数据
"""

import sys
import os
sys.path.append('.')

from scripts.fred_http import series_search
import json

def search_real_m1():
    """搜索真正的M1数据"""
    
    print("🔍 搜索真正的M1货币供应量数据...")
    print("=" * 60)
    
    # 搜索M1相关的关键词
    search_terms = ['M1SL', 'M1NS', 'CURRENCY', 'CURRCIR', 'DEMDEPSL']
    
    all_m1_series = []
    
    for term in search_terms:
        print(f"\n🔎 搜索关键词: {term}")
        try:
            result = series_search(term, limit=20)
            series_list = result.get('seriess', [])
            
            print(f"  找到 {len(series_list)} 个系列")
            
            for s in series_list[:5]:  # 只显示前5个
                series_id = s.get('id', '')
                title = s.get('title', '')
                frequency = s.get('frequency', '')
                print(f"    {series_id}: {title} ({frequency})")
            
            all_m1_series.extend(series_list)
            
        except Exception as e:
            print(f"  搜索失败: {e}")
    
    # 去重
    unique_series = {}
    for s in all_m1_series:
        series_id = s.get('id', '')
        if series_id not in unique_series:
            unique_series[series_id] = s
    
    print(f"\n📊 总共找到 {len(unique_series)} 个唯一的M1相关系列")
    
    # 显示所有找到的M1系列
    print(f"\n📋 所有M1相关系列:")
    for series_id, s in list(unique_series.items())[:20]:  # 只显示前20个
        title = s.get('title', '')
        frequency = s.get('frequency', '')
        print(f"  {series_id}: {title} ({frequency})")
    
    return list(unique_series.values())

if __name__ == "__main__":
    search_real_m1()
