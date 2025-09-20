#!/usr/bin/env python3
"""
检查Money, Banking, & Finance分类的序列
"""

import yaml
from pathlib import Path

def check_money_banking_series():
    """检查Money, Banking, & Finance相关的序列"""
    
    # 读取catalog配置
    with open('config/catalog_fred.yaml', 'r', encoding='utf-8') as f:
        catalog = yaml.safe_load(f)
    
    series = catalog.get('series', [])
    
    # Money, Banking, & Finance 子分类
    categories = {
        'Interest_Rates': ['DGS', 'DTB', 'SOFR', 'FEDFUNDS', 'DFF'],
        'Exchange_Rates': ['DTWEX', 'DEX', 'EXUSEU', 'EXUSUK'],
        'Monetary_Data': ['M1', 'M2', 'M3', 'BASE', 'RESBALNS'],
        'Financial_Indicators': ['VIX', 'NFCI', 'TEDRATE', 'LIBOR'],
        'Banking': ['TOTRESNS', 'TOTLL', 'TOTBKCR', 'TOTALSA'],
        'Business_Lending': ['BAML', 'BAMLEM', 'BAMLH0A0HYM2'],
        'Foreign_Exchange_Intervention': ['FXI', 'INTGSB', 'INTGSN']
    }
    
    print("Money, Banking, & Finance 分类检查")
    print("=" * 50)
    
    categorized_series = {}
    uncategorized = []
    
    for item in series:
        series_id = item.get('id', '')
        alias = item.get('alias', '')
        
        # 检查属于哪个子分类
        found_category = None
        for category, keywords in categories.items():
            if any(keyword in series_id.upper() for keyword in keywords):
                found_category = category
                break
        
        if found_category:
            if found_category not in categorized_series:
                categorized_series[found_category] = []
            categorized_series[found_category].append({
                'id': series_id,
                'alias': alias
            })
        else:
            uncategorized.append({
                'id': series_id,
                'alias': alias
            })
    
    # 显示分类结果
    for category, series_list in categorized_series.items():
        print(f"\n{category} ({len(series_list)} 个序列):")
        for item in series_list:
            print(f"  {item['id']}: {item['alias']}")
    
    print(f"\n未分类序列 ({len(uncategorized)} 个):")
    for item in uncategorized:
        print(f"  {item['id']}: {item['alias']}")
    
    # 检查目录结构
    print(f"\n目录结构检查:")
    data_dir = Path("data/fred")
    
    for category in categories.keys():
        category_dir = data_dir / "categories" / "Money_Banking_Finance" / category
        if category_dir.exists():
            print(f"  ✅ {category}: {category_dir}")
        else:
            print(f"  ❌ {category}: 目录不存在")
    
    return categorized_series

if __name__ == "__main__":
    check_money_banking_series()
