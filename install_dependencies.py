#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
安装PDF和长图生成所需的依赖
"""

import subprocess
import sys
import os

def install_python_package(package):
    """安装Python包"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"✅ {package} 安装成功")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {package} 安装失败: {e}")
        return False

def check_imagemagick():
    """检查ImageMagick是否已安装"""
    try:
        result = subprocess.run(["magick", "-version"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ ImageMagick 已安装")
            return True
        else:
            print("❌ ImageMagick 未安装")
            return False
    except FileNotFoundError:
        print("❌ ImageMagick 未找到")
        return False

def main():
    print("🔧 安装PDF和长图生成依赖...")
    print("=" * 50)
    
    # 安装Python包
    packages = [
        "weasyprint",  # PDF生成
        "wkhtmltopdf", # PDF生成（备用）
    ]
    
    success_count = 0
    for package in packages:
        if install_python_package(package):
            success_count += 1
    
    print("\n📋 安装结果:")
    print(f"  Python包: {success_count}/{len(packages)} 成功")
    
    # 检查ImageMagick
    imagemagick_ok = check_imagemagick()
    
    print("\n💡 手动安装说明:")
    if not imagemagick_ok:
        print("  ImageMagick:")
        print("    1. 访问: https://imagemagick.org/script/download.php")
        print("    2. 下载Windows版本")
        print("    3. 安装时选择'Add to PATH'")
        print("    4. 重启命令行")
    
    print("\n🎯 使用说明:")
    print("  安装完成后，重新运行 crisis_monitor.py")
    print("  系统将自动生成:")
    print("    - PDF报告")
    print("    - 长图版本（手机友好）")

if __name__ == "__main__":
    main()
