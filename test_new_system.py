#!/usr/bin/env python3
"""
测试新的MacroLab系统

验证FRED数据同步、事实表渲染和AI分析功能。
"""

import os
import sys
import subprocess
from datetime import datetime

def test_imports():
    """测试模块导入"""
    print("测试模块导入...")
    try:
        import pandas as pd
        import numpy as np
        import yaml
        import duckdb
        from fredapi import Fred
        from jinja2 import Template
        print("✓ 所有依赖模块导入成功")
        return True
    except ImportError as e:
        print(f"✗ 模块导入失败: {e}")
        return False

def test_fred_connection():
    """测试FRED连接"""
    print("\n测试FRED API连接...")
    try:
        from fredapi import Fred
        from dotenv import load_dotenv
        
        load_dotenv("macrolab.env")
        api_key = os.getenv("FRED_API_KEY")
        
        if not api_key:
            print("✗ 未设置FRED_API_KEY")
            return False
        
        fred = Fred(api_key=api_key)
        
        # 测试获取VIX数据
        vix_data = fred.get_series("VIXCLS")
        if not vix_data.empty:
            print(f"✓ FRED连接成功，获取到{len(vix_data)}条VIX数据")
            print(f"最新VIX值: {vix_data.iloc[-1]:.2f}")
            return True
        else:
            print("✗ 未获取到VIX数据")
            return False
            
    except Exception as e:
        print(f"✗ FRED连接失败: {e}")
        return False

def test_sync_script():
    """测试同步脚本"""
    print("\n测试FRED数据同步脚本...")
    try:
        result = subprocess.run([
            sys.executable, "scripts/sync_fred.py"
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            print("✓ FRED数据同步成功")
            return True
        else:
            print(f"✗ FRED数据同步失败: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ FRED数据同步超时")
        return False
    except Exception as e:
        print(f"✗ FRED数据同步异常: {e}")
        return False

def test_render_script():
    """测试渲染脚本"""
    print("\n测试事实表渲染脚本...")
    try:
        result = subprocess.run([
            sys.executable, "scripts/render_fact_sheets.py"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✓ 事实表渲染成功")
            return True
        else:
            print(f"✗ 事实表渲染失败: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ 事实表渲染超时")
        return False
    except Exception as e:
        print(f"✗ 事实表渲染异常: {e}")
        return False

def test_ai_script():
    """测试AI分析脚本"""
    print("\n测试AI分析脚本...")
    try:
        result = subprocess.run([
            sys.executable, "scripts/ai_assess.py"
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✓ AI分析脚本运行成功")
            return True
        else:
            print(f"✗ AI分析脚本失败: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ AI分析脚本超时")
        return False
    except Exception as e:
        print(f"✗ AI分析脚本异常: {e}")
        return False

def check_output_files():
    """检查输出文件"""
    print("\n检查输出文件...")
    
    # 检查数据目录
    data_dir = "data/fred/series"
    if not os.path.exists(data_dir):
        print("✗ 数据目录不存在")
        return False
    
    # 检查是否有序列目录
    series_dirs = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    if not series_dirs:
        print("✗ 未找到序列目录")
        return False
    
    print(f"✓ 找到 {len(series_dirs)} 个序列目录")
    
    # 检查第一个序列的文件
    first_series = series_dirs[0]
    series_path = os.path.join(data_dir, first_series)
    
    required_files = ["meta.json", "raw.csv", "features.parquet", "fact_sheet.md"]
    for file_name in required_files:
        file_path = os.path.join(series_path, file_name)
        if os.path.exists(file_path):
            print(f"✓ {first_series}/{file_name} 存在")
        else:
            print(f"✗ {first_series}/{file_name} 不存在")
            return False
    
    # 检查DuckDB文件
    db_file = "data/lake/fred.duckdb"
    if os.path.exists(db_file):
        print(f"✓ DuckDB文件存在: {db_file}")
    else:
        print(f"✗ DuckDB文件不存在: {db_file}")
        return False
    
    # 检查输出报告
    output_dir = "outputs/macro_status"
    if os.path.exists(output_dir):
        report_files = [f for f in os.listdir(output_dir) if f.endswith('.md')]
        if report_files:
            print(f"✓ 找到 {len(report_files)} 个分析报告")
        else:
            print("✗ 未找到分析报告")
            return False
    else:
        print("✗ 输出目录不存在")
        return False
    
    return True

def main():
    """主函数"""
    print("MacroLab 新系统测试")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_fred_connection,
        test_sync_script,
        test_render_script,
        test_ai_script,
        check_output_files
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
        print()
    
    print("=" * 50)
    print(f"测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("✓ 所有测试通过！新系统运行正常。")
        print("\n可以运行以下命令:")
        print("  python scripts/sync_fred.py     # 同步FRED数据")
        print("  python scripts/render_fact_sheets.py  # 渲染事实表")
        print("  python scripts/ai_assess.py     # AI分析")
        print("  scripts\\run_daily.bat           # 一键运行")
    else:
        print("✗ 部分测试失败，请检查配置和依赖。")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
