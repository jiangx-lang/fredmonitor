#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试Playwright HTML转图片
"""

import os
from playwright.sync_api import sync_playwright

def simple_test():
    html_file = "outputs/crisis_monitor/crisis_report_20250929_201536.html"
    output_file = "simple_test.png"
    
    print("开始测试...")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            html_path = os.path.abspath(html_file)
            print(f"打开文件: {html_path}")
            
            page.goto(f'file:///{html_path}')
            print("页面加载完成")
            
            page.screenshot(path=output_file, full_page=True)
            print("截图完成")
            
            browser.close()
            
            if os.path.exists(output_file):
                size = os.path.getsize(output_file) / 1024
                print(f"成功！文件大小: {size:.1f} KB")
                return True
            else:
                print("失败：文件未生成")
                return False
                
    except Exception as e:
        print(f"错误: {e}")
        return False

if __name__ == "__main__":
    simple_test()