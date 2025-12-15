#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试FRED数据下载脚本
"""

import subprocess
import sys
import time
import os

def test_fred_download():
    """测试FRED数据下载"""
    print("开始测试FRED数据下载...")
    
    # 检查脚本是否存在
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "sync_fred_http.py")
    if not os.path.exists(script_path):
        print(f"脚本不存在: {script_path}")
        return False
    
    print(f"找到脚本: {script_path}")
    
    # 运行脚本（带超时）
    try:
        print("启动下载脚本...")
        start_time = time.time()
        
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=300,  # 5分钟超时
            encoding='utf-8',
            errors='replace'
        )
        
        elapsed = time.time() - start_time
        print(f"执行时间: {elapsed:.1f}秒")
        
        if result.returncode == 0:
            print("下载成功!")
            print("输出摘要:")
            if result.stdout:
                lines = result.stdout.split('\n')
                for line in lines[-10:]:  # 显示最后10行
                    if line.strip():
                        print(f"  {line}")
            return True
        else:
            print(f"下载失败，返回码: {result.returncode}")
            print("错误输出:")
            if result.stderr:
                print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("下载超时（5分钟）")
        return False
    except Exception as e:
        print(f"执行异常: {e}")
        return False

if __name__ == "__main__":
    success = test_fred_download()
    if success:
        print("\n测试通过!")
    else:
        print("\n测试失败!")
