#!/usr/bin/env python3
"""
测试脚本 - 展示Cursor vs VS Code运行环境的差别
"""

import os
import sys
from pathlib import Path

print("=" * 60)
print("🔍 环境信息对比")
print("=" * 60)

# 1. 工作目录
print(f"📁 当前工作目录: {os.getcwd()}")
print(f"📁 脚本文件路径: {__file__}")
print(f"📁 脚本所在目录: {os.path.dirname(__file__)}")

# 2. Python解释器
print(f"🐍 Python解释器: {sys.executable}")
print(f"🐍 Python版本: {sys.version}")

# 3. 环境变量
print(f"🔑 FRED_API_KEY: {os.getenv('FRED_API_KEY', '未设置')[:10]}...")

# 4. 文件路径测试
env_file = "macrolab.env"
env_file_abs = os.path.join(os.path.dirname(__file__), "macrolab.env")
print(f"📄 相对路径查找: {env_file} -> {os.path.exists(env_file)}")
print(f"📄 绝对路径查找: {env_file_abs} -> {os.path.exists(env_file_abs)}")

# 5. 项目结构
print(f"📂 项目根目录: {Path(__file__).parent}")
print(f"📂 配置文件目录: {Path(__file__).parent / 'config'}")

# 6. 导入测试
try:
    from dotenv import load_dotenv
    print("✅ dotenv模块导入成功")
    
    # 测试环境变量加载
    load_dotenv("macrolab.env")
    api_key = os.getenv("FRED_API_KEY")
    print(f"✅ 环境变量加载: {'成功' if api_key else '失败'}")
    
except ImportError as e:
    print(f"❌ dotenv模块导入失败: {e}")
except Exception as e:
    print(f"❌ 环境变量加载失败: {e}")

print("=" * 60)
