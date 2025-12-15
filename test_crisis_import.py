#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试crisis_monitor.py的导入和基本功能
"""

import sys
import os

print("开始测试crisis_monitor.py...")
print(f"Python版本: {sys.version}")
print(f"当前目录: {os.getcwd()}")

try:
    print("1. 测试基础导入...")
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import yaml
    import requests
    print("OK 基础依赖导入成功")
    
    print("2. 测试项目模块导入...")
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    from core.fred_client import FredClient
    from core.cache import CacheManager
    print("OK 项目模块导入成功")
    
    print("3. 测试环境变量...")
    from dotenv import load_dotenv
    load_dotenv('macrolab.env')
    
    api_key = os.getenv("FRED_API_KEY")
    if api_key:
        print(f"✓ FRED API密钥已设置: {api_key[:8]}...")
    else:
        print("⚠️ FRED API密钥未设置")
    
    print("4. 测试crisis_monitor.py导入...")
    # 尝试导入crisis_monitor的主要函数
    import crisis_monitor
    print("✓ crisis_monitor.py导入成功")
    
    print("5. 测试基本功能...")
    # 检查是否有main函数
    if hasattr(crisis_monitor, 'main'):
        print("✓ 找到main函数")
    else:
        print("⚠️ 未找到main函数")
    
    print("所有测试通过！")
    
except Exception as e:
    print(f"❌ 测试失败: {e}")
    import traceback
    traceback.print_exc()
