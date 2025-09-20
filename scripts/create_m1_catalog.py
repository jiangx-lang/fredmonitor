#!/usr/bin/env python3
"""
创建真正的M1货币供应量数据目录
"""

import os
import yaml
import pathlib
from typing import Dict, Any, List
from dotenv import load_dotenv

# 加载环境变量
BASE = os.getenv("BASE_DIR", os.getcwd())
load_dotenv("macrolab.env")

# 真正的M1货币供应量系列
REAL_M1_SERIES = [
    {
        'id': 'M1SL',
        'alias': 'M1 Money Stock (Seasonally Adjusted)',
        'description': 'M1货币供应量（季节性调整）',
        'frequency': 'Monthly',
        'units': 'Billions of Dollars',
        'seasonal_adjustment': 'Seasonally Adjusted'
    },
    {
        'id': 'M1NS',
        'alias': 'M1 Money Stock (Not Seasonally Adjusted)',
        'description': 'M1货币供应量（非季节性调整）',
        'frequency': 'Monthly',
        'units': 'Billions of Dollars',
        'seasonal_adjustment': 'Not Seasonally Adjusted'
    },
    {
        'id': 'M1REAL',
        'alias': 'Real M1 Money Stock',
        'description': '实际M1货币供应量',
        'frequency': 'Monthly',
        'units': 'Billions of 1982-84 Dollars',
        'seasonal_adjustment': 'Seasonally Adjusted'
    },
    {
        'id': 'CURRENCY',
        'alias': 'Currency Component of M1 (DISCONTINUED)',
        'description': 'M1货币组成部分（已停止）',
        'frequency': 'Weekly',
        'units': 'Billions of Dollars',
        'seasonal_adjustment': 'Seasonally Adjusted'
    },
    {
        'id': 'CURRCIR',
        'alias': 'Currency in Circulation',
        'description': '流通中的货币',
        'frequency': 'Monthly',
        'units': 'Billions of Dollars',
        'seasonal_adjustment': 'Not Seasonally Adjusted'
    },
    {
        'id': 'DEMDEPSL',
        'alias': 'Demand Deposits',
        'description': '活期存款',
        'frequency': 'Monthly',
        'units': 'Billions of Dollars',
        'seasonal_adjustment': 'Seasonally Adjusted'
    },
    {
        'id': 'SAVINGSL',
        'alias': 'Savings Deposits: Total (DISCONTINUED)',
        'description': '储蓄存款总额（已停止）',
        'frequency': 'Monthly',
        'units': 'Billions of Dollars',
        'seasonal_adjustment': 'Seasonally Adjusted'
    },
    {
        'id': 'TCDSL',
        'alias': 'Total Checkable Deposits (DISCONTINUED)',
        'description': '可开支票存款总额（已停止）',
        'frequency': 'Monthly',
        'units': 'Billions of Dollars',
        'seasonal_adjustment': 'Seasonally Adjusted'
    }
]

def create_m1_catalog():
    """创建M1数据目录"""
    
    print("📝 创建真正的M1货币供应量数据目录...")
    print("=" * 60)
    
    # 构建目录结构
    catalog = {
        'metadata': {
            'name': 'M1 Money Stock and Components',
            'description': 'M1货币供应量及其组成部分数据',
            'category_id': 17,
            'subcategory': 'M1_Components',
            'total_series': len(REAL_M1_SERIES),
            'created_at': '2025-09-13',
            'note': '真正的M1货币供应量数据，不是服务贸易数据'
        },
        'series': []
    }
    
    for s in REAL_M1_SERIES:
        series_config = {
            'id': s['id'],
            'alias': s['alias'],
            'description': s['description'],
            'category_id': 17,
            'subcategory': 'M1_Components',
            'frequency': s['frequency'],
            'units': s['units'],
            'seasonal_adjustment': s['seasonal_adjustment'],
            'freshness_days': 7  # M1数据更新频繁
        }
        catalog['series'].append(series_config)
        print(f"✓ 添加系列: {s['id']} - {s['alias']}")
    
    # 保存目录文件
    catalog_file = pathlib.Path(BASE) / "config" / "real_m1_catalog.yaml"
    with open(catalog_file, 'w', encoding='utf-8') as f:
        yaml.dump(catalog, f, default_flow_style=False, allow_unicode=True)
    
    print(f"\n💾 M1目录已保存到: {catalog_file}")
    print(f"📊 包含 {len(REAL_M1_SERIES)} 个真正的M1系列")
    
    return catalog_file

def main():
    """主函数"""
    
    catalog_file = create_m1_catalog()
    
    print(f"\n✅ M1数据目录创建完成!")
    print(f"📁 目录文件: {catalog_file}")
    print(f"\n🎯 下一步:")
    print(f"python -m scripts.sync_real_m1_data  # 下载真正的M1数据")

if __name__ == "__main__":
    main()
