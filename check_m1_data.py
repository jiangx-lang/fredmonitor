#!/usr/bin/env python3
"""
检查M1相关数据的下载情况
"""

import yaml
from pathlib import Path

def check_m1_data():
    """检查M1相关数据"""
    
    # 读取money_banking_catalog
    with open('config/money_banking_catalog.yaml', 'r', encoding='utf-8') as f:
        catalog = yaml.safe_load(f)
    
    series = catalog.get('series', [])
    
    print("🔍 检查M1相关数据...")
    print("=" * 50)
    
    # 查找M1相关系列
    m1_series = []
    for s in series:
        series_id = s.get('id', '')
        alias = s.get('alias', '')
        if 'M1' in series_id or 'M1' in alias:
            m1_series.append(s)
    
    print(f"📊 M1相关系列数量: {len(m1_series)}")
    
    if m1_series:
        print("\n📋 M1相关系列列表:")
        for s in m1_series:
            print(f"  {s['id']}: {s.get('alias', '')}")
    else:
        print("\n❌ 未找到M1相关系列")
    
    # 检查本地M1数据文件
    print(f"\n📁 检查本地M1数据文件...")
    
    # 检查Monetary_Data/series目录
    monetary_series_path = Path("data/fred/categories/Monetary_Data/series")
    if monetary_series_path.exists():
        m1_dirs = [d for d in monetary_series_path.iterdir() if d.is_dir() and 'M1' in d.name]
        print(f"  Monetary_Data/series中的M1目录: {len(m1_dirs)}")
        for d in m1_dirs:
            print(f"    {d.name}")
    
    # 检查M1_Components/series目录
    m1_components_path = Path("data/fred/categories/Monetary_Data/M1_Components/series")
    if m1_components_path.exists():
        m1_comp_dirs = [d for d in m1_components_path.iterdir() if d.is_dir()]
        print(f"  M1_Components/series中的目录: {len(m1_comp_dirs)}")
        for d in m1_comp_dirs:
            print(f"    {d.name}")
    else:
        print("  M1_Components/series目录不存在")
    
    # 检查所有series目录中的M1相关文件
    print(f"\n🔍 搜索所有M1相关数据文件...")
    all_series_path = Path("data/fred/categories")
    m1_files = []
    
    for series_dir in all_series_path.rglob("series"):
        for series_subdir in series_dir.iterdir():
            if series_subdir.is_dir() and 'M1' in series_subdir.name:
                m1_files.append(series_subdir)
    
    print(f"  找到M1相关数据目录: {len(m1_files)}")
    for f in m1_files:
        print(f"    {f}")

if __name__ == "__main__":
    check_m1_data()
