#!/usr/bin/env python3
"""
HTML转长图工具
使用wkhtmltoimage将HTML报告转换为适合分享的长图
"""

import subprocess
import pathlib
import sys
import argparse
from datetime import datetime

def html_to_long_png(html_path: pathlib.Path, output_path: pathlib.Path, 
                     width: int = 1080, quality: int = 100, 
                     max_height: int = 16000, split: bool = False):
    """
    将HTML文件转换为长图PNG
    
    Args:
        html_path: HTML文件路径
        output_path: 输出PNG文件路径
        width: 图片宽度
        quality: 图片质量 (0-100)
        max_height: 最大高度，超过则分割
        split: 是否自动分割过长的图片
    """
    
    # 检查HTML文件是否存在
    if not html_path.exists():
        print(f"❌ HTML文件不存在: {html_path}")
        return False
    
    # 创建输出目录
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # 尝试找到wkhtmltoimage可执行文件
        import pdfkit
        import os
        
        # 获取pdfkit的安装路径
        pdfkit_path = os.path.dirname(pdfkit.__file__)
        wkhtmltoimage_path = os.path.join(pdfkit_path, "bin", "wkhtmltoimage.exe")
        
        # 如果pdfkit路径下没有，尝试系统PATH
        if not os.path.exists(wkhtmltoimage_path):
            wkhtmltoimage_path = "wkhtmltoimage"
        
        # 构建wkhtmltoimage命令
        cmd = [
            wkhtmltoimage_path,
            "--quality", str(quality),
            "--width", str(width),
            "--disable-smart-shrinking",
            "--format", "png",
            str(html_path),
            str(output_path)
        ]
        
        print(f"🔄 正在生成 PNG 长图...")
        print(f"   输入: {html_path}")
        print(f"   输出: {output_path}")
        print(f"   宽度: {width}px, 质量: {quality}")
        
        # 执行命令
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            if output_path.exists():
                file_size = output_path.stat().st_size / 1024  # KB
                print(f"✅ PNG长图生成成功!")
                print(f"   文件大小: {file_size:.1f} KB")
                print(f"   路径: {output_path}")
                return True
            else:
                print(f"❌ PNG文件未生成")
                return False
        else:
            print(f"❌ wkhtmltoimage 执行失败:")
            print(f"   错误: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"❌ 转换超时 (60秒)")
        return False
    except FileNotFoundError:
        print(f"❌ wkhtmltoimage 未找到")
        print(f"   请安装 wkhtmltopdf: pip install wkhtmltopdf")
        print(f"   或下载安装包: https://wkhtmltopdf.org/downloads.html")
        return False
    except Exception as e:
        print(f"❌ 转换失败: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="HTML转长图工具")
    parser.add_argument("html_file", help="HTML文件路径")
    parser.add_argument("--out", "-o", help="输出PNG文件路径")
    parser.add_argument("--width", "-w", type=int, default=1080, help="图片宽度 (默认: 1080)")
    parser.add_argument("--quality", "-q", type=int, default=100, help="图片质量 0-100 (默认: 100)")
    parser.add_argument("--max-height", type=int, default=16000, help="最大高度 (默认: 16000)")
    parser.add_argument("--split", action="store_true", help="自动分割过长图片")
    
    args = parser.parse_args()
    
    # 处理文件路径
    html_path = pathlib.Path(args.html_file)
    
    if args.out:
        output_path = pathlib.Path(args.out)
    else:
        # 默认输出路径
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = html_path.parent / f"{html_path.stem}_long_{timestamp}.png"
    
    # 执行转换
    success = html_to_long_png(
        html_path=html_path,
        output_path=output_path,
        width=args.width,
        quality=args.quality,
        max_height=args.max_height,
        split=args.split
    )
    
    if success:
        print(f"\n🎉 长图生成完成!")
        print(f"📱 适合分享到朋友圈/微信群")
        sys.exit(0)
    else:
        print(f"\n❌ 长图生成失败")
        sys.exit(1)

if __name__ == "__main__":
    main()
