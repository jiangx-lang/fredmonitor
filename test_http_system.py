#!/usr/bin/env python3
"""
测试基于HTTP API的MacroLab系统

验证FRED HTTP API连接、数据同步和事实表渲染功能。
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
        import requests
        from jinja2 import Template
        from tenacity import retry
        print("✓ 所有依赖模块导入成功")
        return True
    except ImportError as e:
        print(f"✗ 模块导入失败: {e}")
        return False

def test_fred_http_connection():
    """测试FRED HTTP API连接"""
    print("\n测试FRED HTTP API连接...")
    try:
        from scripts.fred_http import test_connection
        return test_connection()
    except Exception as e:
        print(f"✗ FRED HTTP API连接失败: {e}")
        return False

def test_sync_http_script():
    """测试HTTP同步脚本"""
    print("\n测试FRED HTTP数据同步脚本...")
    try:
        # 延长超时时间到10分钟，因为并行处理仍然需要时间
        result = subprocess.run([
            sys.executable, "scripts/sync_fred_http.py"
        ], capture_output=True, text=True, timeout=600)
        
        if result.returncode == 0:
            print("✓ FRED HTTP数据同步成功")
            return True
        else:
            print(f"✗ FRED HTTP数据同步失败: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ FRED HTTP数据同步超时（10分钟）")
        return False
    except Exception as e:
        print(f"✗ FRED HTTP数据同步异常: {e}")
        return False

def test_render_http_script():
    """测试HTTP渲染脚本"""
    print("\n测试事实表渲染脚本...")
    try:
        result = subprocess.run([
            sys.executable, "scripts/render_fact_sheets_http.py"
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
    
    # 检查元数据内容
    meta_file = os.path.join(series_path, "meta.json")
    try:
        import json
        with open(meta_file, "r", encoding="utf-8") as f:
            meta = json.load(f)
        
        # 检查关键字段
        key_fields = ["id", "title", "units", "frequency", "last_updated"]
        for field in key_fields:
            if field in meta:
                print(f"✓ {first_series} 元数据包含 {field}")
            else:
                print(f"✗ {first_series} 元数据缺少 {field}")
        
        # 检查下次发布日期
        if "next_release" in meta:
            print(f"✓ {first_series} 下次发布日期: {meta['next_release']}")
        
    except Exception as e:
        print(f"✗ 检查元数据失败: {e}")
        return False
    
    # 检查事实表内容
    fact_sheet_file = os.path.join(series_path, "fact_sheet.md")
    try:
        with open(fact_sheet_file, "r", encoding="utf-8") as f:
            content = f.read()
        
        # 检查关键内容
        key_content = ["**Observations**:", "**Next Release**:", "YoY:", "MoM:", "Official Notes"]
        for key in key_content:
            if key in content:
                print(f"✓ {first_series} 事实表包含 {key}")
            else:
                print(f"✗ {first_series} 事实表缺少 {key}")
        
    except Exception as e:
        print(f"✗ 检查事实表失败: {e}")
        return False
    
    return True

def test_specific_series():
    """测试特定序列（CPI和VIX）"""
    print("\n测试特定序列...")
    
    # 检查CPI序列
    cpi_dir = "data/fred/series/CPIAUCSL"
    if os.path.exists(cpi_dir):
        print("✓ CPI序列目录存在")
        
        # 检查CPI特征数据
        features_file = os.path.join(cpi_dir, "features.parquet")
        if os.path.exists(features_file):
            try:
                import pandas as pd
                df = pd.read_parquet(features_file)
                if "yoy" in df.columns and "mom" in df.columns:
                    print("✓ CPI序列包含YoY和MoM特征")
                else:
                    print("✗ CPI序列缺少YoY或MoM特征")
            except Exception as e:
                print(f"✗ 检查CPI特征数据失败: {e}")
    else:
        print("✗ CPI序列目录不存在")
    
    # 检查VIX序列
    vix_dir = "data/fred/series/VIXCLS"
    if os.path.exists(vix_dir):
        print("✓ VIX序列目录存在")
    else:
        print("✗ VIX序列目录不存在")

def main():
    """主函数"""
    print("MacroLab HTTP API 系统测试")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_fred_http_connection,
        test_sync_http_script,
        test_render_http_script,
        check_output_files,
        test_specific_series
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
        print("✓ 所有测试通过！HTTP API系统运行正常。")
        print("\n可以运行以下命令:")
        print("  python scripts/sync_fred_http.py        # 同步FRED数据")
        print("  python scripts/render_fact_sheets_http.py  # 渲染事实表")
        print("  scripts\\run_daily.bat                   # 一键运行")
    else:
        print("✗ 部分测试失败，请检查配置和依赖。")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
