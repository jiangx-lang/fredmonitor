#!/usr/bin/env python3
"""
D盘工作环境设置脚本
"""

import os
import sys
import pathlib

def setup_d_drive_environment():
    """设置D盘工作环境"""
    
    print("🚀 设置D盘工作环境...")
    print("=" * 80)
    
    # 设置工作目录
    work_dir = pathlib.Path("D:\fred_crisis_monitor")
    os.chdir(work_dir)
    
    print(f"📂 工作目录: {work_dir}")
    
    # 添加项目路径到Python路径
    if str(work_dir) not in sys.path:
        sys.path.insert(0, str(work_dir))
    
    print(f"🐍 Python路径已更新")
    
    # 检查关键文件
    key_files = [
        "scripts/crisis_monitor.py",
        "config/crisis_indicators.yaml", 
        "config/crisis_periods.yaml",
        "data/fred/categories"
    ]
    
    print(f"\n🔍 检查关键文件:")
    for file_path in key_files:
        full_path = work_dir / file_path
        if full_path.exists():
            print(f"  ✅ {file_path}")
        else:
            print(f"  ❌ {file_path}")
    
    print(f"\n🎉 D盘环境设置完成！")
    print(f"📂 当前工作目录: {os.getcwd()}")
    
    return True

if __name__ == "__main__":
    setup_d_drive_environment()
