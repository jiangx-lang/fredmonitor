#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试HTML转图片功能
"""

import os
import time
from playwright.sync_api import sync_playwright

def test_html_to_image():
    """测试HTML转图片功能"""
    
    # 使用最新的HTML文件
    html_file = "outputs/crisis_monitor/crisis_report_20250929_201536.html"
    output_file = "test_playwright_image.png"
    
    if not os.path.exists(html_file):
        print(f"❌ HTML文件不存在: {html_file}")
        return False
    
    try:
        print("🚀 开始使用Playwright转换HTML为图片...")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # 打开HTML文件
            html_abs_path = os.path.abspath(html_file)
            print(f"📄 打开HTML文件: {html_abs_path}")
            
            page.goto(f'file:///{html_abs_path}')
            
            # 等待页面加载
            print("⏳ 等待页面加载...")
            page.wait_for_load_state('networkidle')
            
            # 获取页面实际高度
            page_height = page.evaluate('document.body.scrollHeight')
            print(f"📏 页面高度: {page_height}px")
            
            # 设置视口大小
            page.set_viewport_size({"width": 1200, "height": page_height + 100})
            
            # 等待一下确保渲染完成
            print("⏳ 等待渲染完成...")
            time.sleep(2)
            
            # 截取整个页面
            print("📸 开始截图...")
            page.screenshot(path=output_file, full_page=True)
            
            browser.close()
            
            # 检查文件大小
            if os.path.exists(output_file):
                file_size = os.path.getsize(output_file) / (1024*1024)
                print(f"✅ Playwright长图生成成功: {output_file} ({file_size:.2f} MB)")
                return True
            else:
                print("❌ 图片文件未生成")
                return False
                
    except Exception as e:
        print(f"❌ Playwright生成失败: {e}")
        return False

if __name__ == "__main__":
    success = test_html_to_image()
    if success:
        print("\n🎉 测试成功！Playwright可以正常生成长图")
    else:
        print("\n💥 测试失败！需要检查问题")
















