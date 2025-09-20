#!/usr/bin/env python3
"""
创建Monetary Data各子分类的目录文件
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
MONETARY_SERIES_DATA = {
    "Monetary_Base": [
        {
            'id': 'BASE',
            'alias': 'St. Louis Adjusted Monetary Base (DISCONTINUED)',
            'description': '圣路易斯调整货币基础（已停止）',
            'frequency': 'Biweekly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        },
        {
            'id': 'RESBALNS',
            'alias': 'Total Reserve Balances Maintained with Federal Reserve Banks (DISCONTINUED)',
            'description': '联邦储备银行维持的总储备余额（已停止）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        },
        {
            'id': 'TOTRESNS',
            'alias': 'Reserves of Depository Institutions: Total',
            'description': '存款机构储备：总额',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        }
    ],
    "M2_Components": [
        {
            'id': 'M2SL',
            'alias': 'M2 Money Stock (Seasonally Adjusted)',
            'description': 'M2货币供应量（季节性调整）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        },
        {
            'id': 'M2NS',
            'alias': 'M2 Money Stock (Not Seasonally Adjusted)',
            'description': 'M2货币供应量（非季节性调整）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Not Seasonally Adjusted'
        },
        {
            'id': 'M2REAL',
            'alias': 'Real M2 Money Stock',
            'description': '实际M2货币供应量',
            'frequency': 'Monthly',
            'units': 'Billions of 1982-84 Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        },
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
    "M3_Components": [
        {
            'id': 'M3SL',
            'alias': 'M3 Money Stock (DISCONTINUED)',
            'description': 'M3货币供应量（已停止）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        },
        {
            'id': 'M3NS',
            'alias': 'M3 Money Stock (DISCONTINUED)',
            'description': 'M3货币供应量（已停止）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Not Seasonally Adjusted'
        }
    ],
    "MZM": [
        {
            'id': 'MZM',
            'alias': 'MZM Money Stock (DISCONTINUED)',
            'description': 'MZM货币供应量（已停止）',
            'frequency': 'Weekly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        },
        {
            'id': 'MZMSL',
            'alias': 'MZM Money Stock (DISCONTINUED)',
            'description': 'MZM货币供应量（已停止）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        },
        {
            'id': 'MZMNS',
            'alias': 'MZM Money Stock (DISCONTINUED)',
            'description': 'MZM货币供应量（已停止）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Not Seasonally Adjusted'
        }
    ],
    "Reserves": [
        {
            'id': 'RESBALNS',
            'alias': 'Total Reserve Balances Maintained with Federal Reserve Banks (DISCONTINUED)',
            'description': '联邦储备银行维持的总储备余额（已停止）',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
            'seasonal_adjustment': 'Seasonally Adjusted'
        },
        {
            'id': 'TOTRESNS',
            'alias': 'Reserves of Depository Institutions: Total',
            'description': '存款机构储备：总额',
            'frequency': 'Monthly',
            'units': 'Billions of Dollars',
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
    
    print("📝 创建Monetary Data各子分类目录...")
    print("=" * 60)
    
    all_catalogs = []
    
    for subcategory_name, series_list in MONETARY_SERIES_DATA.items():
        catalog_file = create_subcategory_catalog(subcategory_name, series_list)
        if catalog_file:
            all_catalogs.append(catalog_file)
    
    print(f"\n✅ Monetary Data子分类目录创建完成!")
    print(f"📊 创建了 {len(all_catalogs)} 个目录文件")
    
    if all_catalogs:
        print(f"\n📁 创建的目录文件:")
        for catalog_file in all_catalogs:
            print(f"  {catalog_file}")
        
        print(f"\n🎯 下一步:")
        print(f"python -m scripts.sync_monetary_subcategories  # 下载所有子分类数据")

if __name__ == "__main__":
    main()
