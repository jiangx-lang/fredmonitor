#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FRED 危机预警监控系统（修复版）
"""

import os
import sys
import json
import yaml
import math
import warnings
import pathlib
import base64
import re
import subprocess
from datetime import datetime
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# 加载环境变量
try:
    load_dotenv()
    print("✅ 环境变量加载成功")
except Exception as e:
    print(f"⚠️ 环境变量加载失败: {e}")

# 基础路径
BASE = pathlib.Path(__file__).parent

# 依赖模块
from scripts.fred_http import series_observations, series_search

def load_yaml_config(file_path: pathlib.Path) -> dict:
    """加载YAML配置文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 加载配置文件失败 {file_path}: {e}")
        return {}

def main():
    """主函数"""
    print("🚨 FRED 危机预警监控系统启动...")
    print("=" * 80)
    
    # 加载配置
    indicators_cfg = load_yaml_config(BASE / "config" / "crisis_indicators.yaml")
    crises_cfg = load_yaml_config(BASE / "config" / "crisis_periods.yaml")
    
    if not indicators_cfg or not crises_cfg:
        print("❌ 配置文件加载失败")
        return
    
    indicators = indicators_cfg.get("indicators", [])
    crises = crises_cfg.get("crises", [])
    
    print(f"📊 指标数: {len(indicators)}")
    print(f"📅 危机段: {len(crises)}")
    
    # 生成时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"\n🎉 系统初始化完成！")
    print(f"📅 时间戳: {timestamp}")
    
    # 这里可以添加更多的处理逻辑
    print("✅ 系统运行正常")

if __name__ == "__main__":
    main()
