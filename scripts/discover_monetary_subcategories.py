#!/usr/bin/env python3
"""
发现Monetary Data所有子分类的数据
"""

import os
import yaml
import pathlib
from typing import Dict, Any, List
from dotenv import load_dotenv

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.fred_http import category_series, polite_sleep

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

# Monetary Data子分类的关键词映射
MONETARY_SUBCATEGORIES = {
    "Monetary_Base": {
        "keywords": ["BASE", "MBASE", "MONETARYBASE", "RESBALNS"],
        "expected_count": 27,
        "priority": 1
    },
    "Reserves": {
        "keywords": ["RESBALNS", "RESERVES", "TOTRESNS", "RESBAL"],
        "expected_count": 61,
        "priority": 1
    },
    "M2_Components": {
        "keywords": ["M2", "M2SL", "M2NS", "M2REAL"],
        "expected_count": 50,
        "priority": 1
    },
    "M3_Components": {
        "keywords": ["M3", "M3SL", "M3NS", "M3REAL"],
        "expected_count": 60,
        "priority": 2
    },
    "MZM": {
        "keywords": ["MZM", "MZMSL", "MZMNS"],
        "expected_count": 10,
        "priority": 2
    },
    "Money_Velocity": {
        "keywords": ["VELOCITY", "VELOCITYM1", "VELOCITYM2", "VELOCITYM3"],
        "expected_count": 3,
        "priority": 2
    },
    "Borrowings": {
        "keywords": ["BORROW", "BORROWINGS", "BORROWED"],
        "expected_count": 18,
        "priority": 3
    },
    "Memorandum_Items": {
        "keywords": ["MEMO", "MEMORANDUM", "MEMORANDUM_ITEMS"],
        "expected_count": 23,
        "priority": 3
    },
    "Factors_Affecting_Reserve_Balances": {
        "keywords": ["FARB", "FACTORS", "RESERVE_BALANCES"],
        "expected_count": 650,
        "priority": 4
    },
    "Securities_Loans_Assets_Liabilities": {
        "keywords": ["SECURITIES", "LOANS", "ASSETS", "LIABILITIES", "FED"],
        "expected_count": 203,
        "priority": 4
    }
}

def discover_subcategory_series(subcategory_name: str, keywords: List[str]) -> List[Dict[str, Any]]:
    """发现特定子分类的数据系列"""
    
    print(f"\n🔍 发现 {subcategory_name} 数据...")
    
    # 获取Monetary Data分类的所有系列
    result = category_series(17, limit=1000)  # 增加限制以获取更多数据
    series_list = result.get('seriess', [])
    
    print(f"  Monetary Data分类总系列数: {len(series_list)}")
    
    # 查找匹配的系列
    matched_series = []
    
    for s in series_list:
        series_id = s.get('id', '')
        title = s.get('title', '')
        
        # 检查是否包含关键词
        if any(keyword in series_id.upper() for keyword in keywords):
            matched_series.append(s)
            print(f"    ✓ {series_id}: {title}")
    
    print(f"  {subcategory_name} 找到 {len(matched_series)} 个系列")
    
    return matched_series

def create_subcategory_catalog(subcategory_name: str, series_list: List[Dict[str, Any]], expected_count: int):
    """创建子分类目录文件"""
    
    if not series_list:
        print(f"  ❌ {subcategory_name} 没有找到数据系列")
        return None
    
    print(f"\n📝 创建 {subcategory_name} 目录...")
    
    # 构建目录结构
    catalog = {
        'metadata': {
            'name': subcategory_name.replace('_', ' '),
            'description': f'{subcategory_name.replace("_", " ")} 数据',
            'category_id': 17,
            'subcategory': subcategory_name,
            'total_series': len(series_list),
            'expected_count': expected_count,
            'created_at': '2025-09-13'
        },
        'series': []
    }
    
    for s in series_list:
        series_config = {
            'id': s.get('id', ''),
            'alias': s.get('title', ''),
            'category_id': 17,
            'subcategory': subcategory_name,
            'frequency': s.get('frequency', ''),
            'units': s.get('units', ''),
            'seasonal_adjustment': s.get('seasonal_adjustment', ''),
            'freshness_days': 7
        }
        catalog['series'].append(series_config)
    
    # 保存目录文件
    catalog_file = pathlib.Path(BASE) / "config" / f"{subcategory_name.lower()}_catalog.yaml"
    with open(catalog_file, 'w', encoding='utf-8') as f:
        yaml.dump(catalog, f, default_flow_style=False, allow_unicode=True)
    
    print(f"  💾 {subcategory_name} 目录已保存到: {catalog_file}")
    
    return catalog_file

def main():
    """主函数"""
    
    print("🔍 发现Monetary Data所有子分类数据...")
    print("=" * 60)
    
    # 按优先级排序
    sorted_subcategories = sorted(
        MONETARY_SUBCATEGORIES.items(), 
        key=lambda x: x[1]['priority']
    )
    
    all_catalogs = []
    
    for subcategory_name, subcategory_info in sorted_subcategories:
        keywords = subcategory_info['keywords']
        expected_count = subcategory_info['expected_count']
        
        # 发现系列
        series_list = discover_subcategory_series(subcategory_name, keywords)
        
        # 创建目录
        catalog_file = create_subcategory_catalog(subcategory_name, series_list, expected_count)
        
        if catalog_file:
            all_catalogs.append(catalog_file)
        
        # 礼貌性延迟
        polite_sleep()
    
    print(f"\n✅ Monetary Data子分类发现完成!")
    print(f"📊 创建了 {len(all_catalogs)} 个目录文件")
    
    if all_catalogs:
        print(f"\n📁 创建的目录文件:")
        for catalog_file in all_catalogs:
            print(f"  {catalog_file}")
        
        print(f"\n🎯 下一步:")
        print(f"python -m scripts.sync_monetary_subcategories  # 下载所有子分类数据")

if __name__ == "__main__":
    main()
