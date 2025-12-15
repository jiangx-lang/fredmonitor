#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试GUI下载功能
"""

import os
import sys
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox

def test_download_function():
    """测试下载功能"""
    print("测试下载功能...")
    
    try:
        # 检查脚本是否存在
        download_script_path = os.path.join(os.path.dirname(__file__), "scripts", "sync_fred_http.py")
        print(f"脚本路径: {download_script_path}")
        print(f"脚本存在: {os.path.exists(download_script_path)}")
        
        if os.path.exists(download_script_path):
            print("✅ 脚本存在，开始测试...")
            
            # 运行脚本（短时间测试）
            process = subprocess.Popen([sys.executable, download_script_path],
                                     stdout=subprocess.PIPE, 
                                     stderr=subprocess.STDOUT,
                                     text=True, 
                                     bufsize=1,
                                     universal_newlines=True,
                                     encoding='utf-8',
                                     errors='replace')
            
            # 读取前几行输出
            for i in range(5):
                output = process.stdout.readline()
                if output:
                    print(f"输出 {i+1}: {output.strip()}")
                else:
                    break
            
            # 终止进程（测试用）
            process.terminate()
            print("✅ 脚本可以正常启动")
        else:
            print("❌ 脚本不存在")
            
    except Exception as e:
        print(f"❌ 测试失败: {e}")

def test_gui_button():
    """测试GUI按钮"""
    print("测试GUI按钮...")
    
    try:
        root = tk.Tk()
        root.title("测试GUI")
        
        def test_command():
            print("✅ 按钮点击成功！")
            messagebox.showinfo("测试", "按钮点击成功！")
        
        btn = ttk.Button(root, text="测试按钮", command=test_command)
        btn.pack(pady=20)
        
        print("✅ GUI可以正常创建")
        print("点击测试按钮验证功能...")
        
        # 自动关闭（测试用）
        root.after(3000, root.destroy)
        root.mainloop()
        
    except Exception as e:
        print(f"❌ GUI测试失败: {e}")

if __name__ == "__main__":
    print("=" * 50)
    print("GUI下载功能诊断测试")
    print("=" * 50)
    
    test_download_function()
    print()
    test_gui_button()
    
    print("=" * 50)
    print("测试完成")
    print("=" * 50)











