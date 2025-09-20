#!/usr/bin/env python3
"""
创建剩余Monetary Data子分类的目录文件
基于直接搜索找到的系列
"""

import os
import yaml
import pathlib
from typing import Dict, Any, List
from dotenv import load_dotenv

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

# 基于直接搜索找到的系列
REMAINING_SERIES_DATA = {
    "M2_Minus_Small_Time_Deposits": [
        {
            'id': 'M2MNS',
            'alias': 'M2 Less Small Time Deposits (Not Seasonally Adjusted)',
            'description': 'M2减去小额定期存款（非季节性调整）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Not Seasonally Adjusted'
        },
        {
            'id': 'M2MSL',
            'alias': 'M2 Less Small Time Deposits (Seasonally Adjusted)',
            'description': 'M2减去小额定期存款（季节性调整）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        }
    ],
    "Securities_Loans_Assets_Liabilities": [
        {
            'id': 'LOANS',
            'alias': 'Loans and Leases in Bank Credit, All Commercial Banks',
            'description': '银行信贷中的贷款和租赁，所有商业银行',
            'frequency': 'Monthly',
            'units': 'Billions of U.S. Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        }
    ]
}

def create_subcategory_catalog(subcategory_name: str, series_list: List[Dict[str, Any]]):
    """创建子分类目录文件"""
    
    if not series_list:
        print(f"❌ {subcategory_name} 没有系列数据")
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
            'created_at': '2025-09-13'
        },
        'series': []
    }
    
    for s in series_list:
        series_config = {
            'id': s['id'],
            'alias': s['alias'],
            'description': s['description'],
            'category_id': 17,
            'subcategory': subcategory_name,
            'frequency': s['frequency'],
            'units': s['units'],
            'seasonal_adjustment': s['seasonal_adjustment'],
            'freshness_days': 7
        }
        catalog['series'].append(series_config)
        print(f"  ✓ 添加系列: {s['id']} - {s['alias']}")
    
    # 保存目录文件
    catalog_file = pathlib.Path(BASE) / "config" / f"{subcategory_name.lower()}_catalog.yaml"
    with open(catalog_file, 'w', encoding='utf-8') as f:
        yaml.dump(catalog, f, default_flow_style=False, allow_unicode=True)
    
    print(f"  💾 {subcategory_name} 目录已保存到: {catalog_file}")
    
    return catalog_file

def main():
    """主函数"""
    
    print("📝 创建剩余Monetary Data子分类目录...")
    print("=" * 60)
    
    all_catalogs = []
    
    for subcategory_name, series_list in REMAINING_SERIES_DATA.items():
        catalog_file = create_subcategory_catalog(subcategory_name, series_list)
        if catalog_file:
            all_catalogs.append(catalog_file)
    
    print(f"\n✅ 剩余Monetary Data子分类目录创建完成!")
    print(f"📊 创建了 {len(all_catalogs)} 个目录文件")
    
    if all_catalogs:
        print(f"\n📁 创建的目录文件:")
        for catalog_file in all_catalogs:
            print(f"  {catalog_file}")
        
        print(f"\n🎯 下一步:")
        print(f"python -m scripts.sync_remaining_monetary  # 下载剩余子分类数据")

if __name__ == "__main__":
    main()
