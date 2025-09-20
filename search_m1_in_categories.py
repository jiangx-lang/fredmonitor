#!/usr/bin/env python3
"""
在所有分类中搜索M1数据
"""

import sys
import os
sys.path.append('.')

from scripts.fred_http import category_series
import json

def search_m1_in_categories():
    """在所有分类中搜索M1数据"""
    
    print("🔍 在所有分类中搜索M1数据...")
    print("=" * 60)
    
    # 要检查的分类ID
    categories_to_check = [10, 15, 16, 17, 18, 19]  # Money Banking Finance相关分类
    
    all_m1_series = []
    
    for cat_id in categories_to_check:
        print(f"\n🔎 检查分类 {cat_id}...")
        try:
            result = category_series(cat_id, limit=200)
            series_list = result.get('seriess', [])
            
            print(f"  分类 {cat_id} 总系列数: {len(series_list)}")
            
            # 查找M1相关系列
            m1_series = []
            for s in series_list:
                series_id = s.get('id', '')
                if 'M1' in series_id:
                    m1_series.append(s)
                    print(f"    ✓ {series_id}: {s.get('title', '')}")
            
            if m1_series:
                all_m1_series.extend(m1_series)
                print(f"  找到 {len(m1_series)} 个M1系列")
            else:
                print(f"  未找到M1系列")
                
        except Exception as e:
            print(f"  检查分类 {cat_id} 失败: {e}")
    
    print(f"\n📊 总共找到 {len(all_m1_series)} 个M1相关系列")
    
    if all_m1_series:
        print(f"\n📋 所有M1相关系列:")
        for s in all_m1_series:
            series_id = s.get('id', '')
            title = s.get('title', '')
            frequency = s.get('frequency', '')
            print(f"  {series_id}: {title} ({frequency})")
    
    return all_m1_series

if __name__ == "__main__":
    search_m1_in_categories()
