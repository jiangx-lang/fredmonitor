#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化的crisis_monitor测试版本
跳过数据管道，直接测试核心功能
"""

import os
import sys
import json
import yaml
import warnings
import pathlib
from datetime import datetime
import pytz

# 设置控制台编码为UTF-8
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

# 抑制警告
warnings.filterwarnings("ignore", category=FutureWarning)

# 工程路径
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

# 统一时区设置
JST = pytz.timezone("Asia/Tokyo")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

# 加载环境变量
try:
    env_files = [BASE / "macrolab.env", BASE / ".env"]
    loaded = False
    for env_file in env_files:
        if env_file.exists():
            try:
                load_dotenv(env_file, encoding='utf-8')
                loaded = True
                print(f"OK 环境变量加载成功: {env_file.name}")
                break
            except UnicodeDecodeError:
                try:
                    load_dotenv(env_file, encoding='gbk')
                    loaded = True
                    print(f"OK 环境变量加载成功: {env_file.name}")
                    break
                except:
                    continue
    
    if not loaded:
        print("WARNING 未找到环境变量文件，将使用系统环境变量")
except Exception as e:
    print(f"WARNING 加载环境变量失败: {e}，将使用系统环境变量")

def load_yaml_config(file_path):
    """加载YAML配置文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"ERROR 加载配置文件失败: {e}")
        return {}

def test_basic_functionality():
    """测试基本功能"""
    print("开始测试基本功能...")
    
    # 1. 测试配置文件加载
    print("1. 测试配置文件加载...")
    config_path = BASE / "config" / "crisis_indicators.yaml"
    if config_path.exists():
        config = load_yaml_config(config_path)
        indicators = config.get('indicators', [])
        print(f"OK 加载了 {len(indicators)} 个指标")
    else:
        print(f"ERROR 配置文件不存在: {config_path}")
        return False
    
    # 2. 测试FRED模块导入
    print("2. 测试FRED模块导入...")
    try:
        from scripts.fred_http import series_observations, series_search
        from scripts.clean_utils import parse_numeric_series
        print("OK FRED模块导入成功")
    except ImportError as e:
        print(f"WARNING FRED模块不可用: {e}")
    
    # 3. 测试数据目录
    print("3. 测试数据目录...")
    data_dir = BASE / "data"
    if data_dir.exists():
        print(f"OK 数据目录存在: {data_dir}")
    else:
        print(f"WARNING 数据目录不存在: {data_dir}")
    
    # 4. 测试输出目录
    print("4. 测试输出目录...")
    output_dir = BASE / "outputs" / "crisis_monitor"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"OK 输出目录准备完成: {output_dir}")
    
    return True

def main():
    """主函数"""
    print("FRED 危机预警监控系统 - 简化测试版本")
    print("=" * 80)
    
    try:
        if test_basic_functionality():
            print("所有基本功能测试通过！")
            print("系统可以正常运行，问题可能在于数据管道或网络请求")
        else:
            print("基本功能测试失败")
    except Exception as e:
        print(f"ERROR 测试过程中出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()









