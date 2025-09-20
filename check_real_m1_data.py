#!/usr/bin/env python3
"""
检查真正的M1货币供应量数据
"""

import sys
import os
sys.path.append('.')

from scripts.fred_http import category_series
import json

def check_real_m1_data():
    """检查真正的M1数据"""
    
    print("🔍 检查真正的M1货币供应量数据...")
    print("=" * 60)
    
    # 获取Monetary Data分类的所有系列
    result = category_series(17, limit=200)
    series_list = result.get('seriess', [])
    
    print(f"📊 Monetary Data分类总系列数: {len(series_list)}")
    
    # 查找真正的M1相关数据
    m1_keywords = ['M1', 'M1SL', 'M1NS', 'CURRENCY', 'CURRCIR', 'DEMDEPSL']
    m1_series = []
    
    for s in series_list:
        series_id = s.get('id', '')
        title = s.get('title', '')
        
        # 检查是否包含M1关键词
        if any(keyword in series_id.upper() for keyword in m1_keywords):
            m1_series.append(s)
    
    print(f"\n📋 找到M1相关系列: {len(m1_series)}")
    
    if m1_series:
        print("\n📝 M1相关系列详情:")
        for s in m1_series[:20]:  # 只显示前20个
            series_id = s.get('id', '')
            title = s.get('title', '')
            frequency = s.get('frequency', '')
            print(f"  {series_id}: {title} ({frequency})")
    
    # 检查我们已下载的数据中是否有真正的M1数据
    print(f"\n📁 检查已下载的M1数据...")
    
    # 检查所有已下载的系列
    import yaml
    with open('config/money_banking_catalog.yaml', 'r', encoding='utf-8') as f:
        catalog = yaml.safe_load(f)
    
    downloaded_series = catalog.get('series', [])
    downloaded_m1 = []
    
    for s in downloaded_series:
        series_id = s.get('id', '')
        if any(keyword in series_id.upper() for keyword in m1_keywords):
            downloaded_m1.append(s)
    
    print(f"  已下载的M1相关系列: {len(downloaded_m1)}")
    for s in downloaded_m1:
        print(f"    {s['id']}: {s.get('alias', '')}")
    
    if len(downloaded_m1) == 0:
        print("\n❌ 问题发现: 我们没有下载真正的M1货币供应量数据!")
        print("   我们下载的都是服务贸易数据(IT开头)，不是M1货币数据")
        print("\n💡 建议:")
        print("   1. 需要重新发现并下载真正的M1货币供应量数据")
        print("   2. M1数据系列ID通常以M1开头，如M1SL, M1NS等")
        print("   3. 或者搜索CURRENCY, CURRCIR等关键词")

if __name__ == "__main__":
    check_real_m1_data()
