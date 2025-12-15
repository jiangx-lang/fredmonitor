#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析系统脱节问题
"""

import os
import sys
import pathlib
import yaml
import pandas as pd

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

def analyze_system_disconnection():
    """分析系统脱节问题"""
    print("🔍 分析系统脱节问题")
    print("=" * 80)
    
    base_dir = pathlib.Path(__file__).parent
    
    # 1. 分析数据路径不一致
    print("\n📁 1. 数据路径不一致问题:")
    
    # HTTP下载程序的数据路径
    http_data_dir = base_dir / "data" / "fred" / "series"
    print(f"   HTTP下载程序路径: {http_data_dir}")
    print(f"   存在: {http_data_dir.exists()}")
    
    # crisis_monitor期望的数据路径
    crisis_data_dir = base_dir / "data" / "series"
    print(f"   crisis_monitor期望路径: {crisis_data_dir}")
    print(f"   存在: {crisis_data_dir.exists()}")
    
    # 2. 分析配置文件不一致
    print("\n📋 2. 配置文件不一致问题:")
    
    # catalog_fred.yaml (HTTP下载程序使用)
    catalog_file = base_dir / "config" / "catalog_fred.yaml"
    if catalog_file.exists():
        with open(catalog_file, 'r', encoding='utf-8') as f:
            catalog = yaml.safe_load(f)
        catalog_series = [s['id'] for s in catalog.get('series', [])]
        print(f"   catalog_fred.yaml 序列数: {len(catalog_series)}")
        print(f"   前5个序列: {catalog_series[:5]}")
    else:
        print("   ❌ catalog_fred.yaml 不存在")
        catalog_series = []
    
    # crisis_indicators.yaml (crisis_monitor使用)
    crisis_file = base_dir / "config" / "crisis_indicators.yaml"
    if crisis_file.exists():
        with open(crisis_file, 'r', encoding='utf-8') as f:
            crisis_config = yaml.safe_load(f)
        crisis_series = [s['id'] for s in crisis_config.get('indicators', [])]
        print(f"   crisis_indicators.yaml 序列数: {len(crisis_series)}")
        print(f"   前5个序列: {crisis_series[:5]}")
    else:
        print("   ❌ crisis_indicators.yaml 不存在")
        crisis_series = []
    
    # 3. 分析数据文件存在情况
    print("\n📊 3. 数据文件存在情况:")
    
    # HTTP下载的数据文件
    http_files = []
    if http_data_dir.exists():
        for series_dir in http_data_dir.iterdir():
            if series_dir.is_dir():
                raw_file = series_dir / "raw.csv"
                if raw_file.exists():
                    http_files.append(series_dir.name)
    
    print(f"   HTTP下载的数据文件数: {len(http_files)}")
    print(f"   前5个文件: {http_files[:5]}")
    
    # crisis_monitor期望的数据文件
    crisis_files = []
    if crisis_data_dir.exists():
        for csv_file in crisis_data_dir.glob("*.csv"):
            crisis_files.append(csv_file.stem)
    
    print(f"   crisis_monitor期望的数据文件数: {len(crisis_files)}")
    print(f"   前5个文件: {crisis_files[:5]}")
    
    # 4. 分析序列ID不匹配
    print("\n🔄 4. 序列ID不匹配问题:")
    
    # 在catalog中但不在crisis中的序列
    missing_in_crisis = set(catalog_series) - set(crisis_series)
    print(f"   在catalog中但不在crisis中的序列: {len(missing_in_crisis)}")
    if missing_in_crisis:
        print(f"   示例: {list(missing_in_crisis)[:5]}")
    
    # 在crisis中但不在catalog中的序列
    missing_in_catalog = set(crisis_series) - set(catalog_series)
    print(f"   在crisis中但不在catalog中的序列: {len(missing_in_catalog)}")
    if missing_in_catalog:
        print(f"   示例: {list(missing_in_catalog)[:5]}")
    
    # 5. 分析数据处理流程脱节
    print("\n⚙️ 5. 数据处理流程脱节:")
    
    # YoY计算脚本
    yoy_script = base_dir / "scripts" / "calculate_yoy_indicators.py"
    print(f"   YoY计算脚本: {yoy_script.exists()}")
    
    # 企业债/GDP计算脚本
    corp_debt_script = base_dir / "scripts" / "calculate_corporate_debt_gdp_ratio.py"
    print(f"   企业债/GDP计算脚本: {corp_debt_script.exists()}")
    
    # 合成指标计算
    synthetic_scripts = [
        "calculate_corporate_debt_gdp_ratio.py",
        "calculate_yoy_indicators.py"
    ]
    
    print("   独立计算脚本:")
    for script in synthetic_scripts:
        script_path = base_dir / "scripts" / script
        print(f"     - {script}: {script_path.exists()}")
    
    # 6. 分析主程序数据依赖
    print("\n🎯 6. 主程序数据依赖:")
    
    # crisis_monitor.py中的数据获取逻辑
    crisis_monitor_file = base_dir / "crisis_monitor.py"
    if crisis_monitor_file.exists():
        with open(crisis_monitor_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 查找数据获取相关代码
        if "fetch_series" in content:
            print("   ✅ 使用fetch_series函数")
        if "CORPORATE_DEBT_GDP_RATIO.csv" in content:
            print("   ✅ 使用企业债/GDP比率文件")
        if "_YOY.csv" in content:
            print("   ✅ 使用YoY数据文件")
        if "data/series" in content:
            print("   ✅ 使用data/series路径")
    
    # 7. 总结脱节问题
    print("\n📝 7. 脱节问题总结:")
    
    issues = []
    
    # 路径不一致
    if http_data_dir != crisis_data_dir:
        issues.append("数据路径不一致：HTTP下载程序使用data/fred/series，crisis_monitor使用data/series")
    
    # 配置不一致
    if len(missing_in_crisis) > 0 or len(missing_in_catalog) > 0:
        issues.append("配置文件不一致：catalog_fred.yaml和crisis_indicators.yaml中的序列ID不匹配")
    
    # 数据处理脱节
    if len(synthetic_scripts) > 0:
        issues.append("数据处理脱节：存在多个独立的计算脚本，没有统一的数据处理流程")
    
    # 数据文件不匹配
    if len(http_files) != len(crisis_files):
        issues.append("数据文件不匹配：HTTP下载的数据文件数量与crisis_monitor期望的不一致")
    
    print(f"   发现 {len(issues)} 个主要脱节问题:")
    for i, issue in enumerate(issues, 1):
        print(f"   {i}. {issue}")
    
    return issues

def main():
    """主函数"""
    print("系统脱节问题分析程序")
    print("=" * 80)
    
    try:
        issues = analyze_system_disconnection()
        
        print("\n" + "=" * 80)
        print("🎯 建议解决方案:")
        print("1. 统一数据路径：将所有数据文件复制到data/series/目录")
        print("2. 统一配置文件：确保catalog_fred.yaml包含crisis_indicators.yaml中的所有序列")
        print("3. 集成数据处理：将YoY计算、合成指标计算集成到主下载程序中")
        print("4. 创建统一数据管道：下载→处理→存储→使用的一体化流程")
        
    except Exception as e:
        print(f"\n❌ 分析过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()









