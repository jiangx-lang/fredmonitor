#!/usr/bin/env python3
"""
发现并下载真正的M1货币供应量数据
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

# M1相关的关键词
M1_KEYWORDS = [
    'M1SL', 'M1NS', 'M1REAL',  # M1货币供应量
    'CURRENCY', 'CURRCIR',     # 货币流通
    'DEMDEPSL', 'DEMDEPSN',    # 活期存款
    'SAVINGS', 'SAVINGSL',     # 储蓄存款
    'TCDSL', 'TCDSN',         # 定期存款
    'MMSL', 'MMNS',           # 货币市场基金
    'VELOCITYM1', 'VELOCITYM2' # 货币流通速度
]

def discover_m1_series():
    """发现M1相关的数据系列"""
    
    print("🔍 发现M1货币供应量数据...")
    print("=" * 60)
    
    # 获取Monetary Data分类的所有系列
    result = category_series(17, limit=500)  # 增加限制以获取更多数据
    series_list = result.get('seriess', [])
    
    print(f"📊 Monetary Data分类总系列数: {len(series_list)}")
    
    # 查找M1相关系列
    m1_series = []
    
    for s in series_list:
        series_id = s.get('id', '')
        title = s.get('title', '')
        
        # 检查是否包含M1关键词
        if any(keyword in series_id.upper() for keyword in M1_KEYWORDS):
            m1_series.append(s)
            print(f"✓ 找到M1系列: {series_id} - {title}")
    
    print(f"\n📋 总共找到 {len(m1_series)} 个M1相关系列")
    
    return m1_series

def create_m1_catalog(m1_series: List[Dict[str, Any]]):
    """创建M1数据目录"""
    
    print(f"\n📝 创建M1数据目录...")
    
    # 构建目录结构
    catalog = {
        'metadata': {
            'name': 'M1 Money Stock and Components',
            'description': 'M1货币供应量及其组成部分数据',
            'category_id': 17,
            'subcategory': 'M1_Components',
            'total_series': len(m1_series),
            'created_at': '2025-09-13'
        },
        'series': []
    }
    
    for s in m1_series:
        series_config = {
            'id': s.get('id', ''),
            'alias': s.get('title', ''),
            'category_id': 17,
            'subcategory': 'M1_Components',
            'frequency': s.get('frequency', ''),
            'units': s.get('units', ''),
            'seasonal_adjustment': s.get('seasonal_adjustment', ''),
            'freshness_days': 7  # M1数据更新频繁
        }
        catalog['series'].append(series_config)
    
    # 保存目录文件
    catalog_file = pathlib.Path(BASE) / "config" / "m1_catalog.yaml"
    with open(catalog_file, 'w', encoding='utf-8') as f:
        yaml.dump(catalog, f, default_flow_style=False, allow_unicode=True)
    
    print(f"💾 M1目录已保存到: {catalog_file}")
    
    return catalog_file

def main():
    """主函数"""
    
    # 发现M1系列
    m1_series = discover_m1_series()
    
    if not m1_series:
        print("❌ 未找到M1相关系列")
        return
    
    # 创建M1目录
    catalog_file = create_m1_catalog(m1_series)
    
    print(f"\n✅ M1数据发现完成!")
    print(f"📊 找到 {len(m1_series)} 个M1相关系列")
    print(f"📁 目录文件: {catalog_file}")
    print(f"\n🎯 下一步:")
    print(f"python -m scripts.sync_m1_data  # 下载M1数据")

if __name__ == "__main__":
    main()
