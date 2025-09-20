#!/usr/bin/env python3
"""
HTML转长图工具 (使用Playwright)
将HTML报告转换为适合分享的长图
"""

import pathlib
import sys
import argparse
from datetime import datetime
import asyncio

async def html_to_long_png_playwright(html_path: pathlib.Path, output_path: pathlib.Path, 
                                     width: int = 1080, quality: int = 100):
    """
    使用Playwright将HTML文件转换为长图PNG
    
    Args:
        html_path: HTML文件路径
        output_path: 输出PNG文件路径
        width: 图片宽度
        quality: 图片质量 (0-100)
    """
    
    # 检查HTML文件是否存在
    if not html_path.exists():
        print(f"❌ HTML文件不存在: {html_path}")
        return False
    
    # 创建输出目录
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        from playwright.async_api import async_playwright
        
        print(f"🔄 正在使用 Playwright 生成 PNG 长图...")
        print(f"   输入: {html_path}")
        print(f"   输出: {output_path}")
        print(f"   宽度: {width}px, 质量: {quality}")
        
        async with async_playwright() as p:
            # 启动浏览器
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # 设置视口大小
            await page.set_viewport_size({"width": width, "height": 1200})
            
            # 加载HTML文件
            await page.goto(f"file://{html_path.absolute()}")
            
            # 等待页面加载完成
            await page.wait_for_load_state("networkidle")
            
            # 获取页面高度
            page_height = await page.evaluate("document.body.scrollHeight")
            print(f"   页面高度: {page_height}px")
            
            # 设置视口高度为页面高度
            await page.set_viewport_size({"width": width, "height": page_height})
            
            # 截取全页面
            await page.screenshot(
                path=str(output_path),
                full_page=True
            )
            
            await browser.close()
        
        if output_path.exists():
            file_size = output_path.stat().st_size / 1024  # KB
            print(f"✅ PNG长图生成成功!")
            print(f"   文件大小: {file_size:.1f} KB")
            print(f"   路径: {output_path}")
            return True
        else:
            print(f"❌ PNG文件未生成")
            return False
            
    except ImportError:
        print(f"❌ Playwright 未安装")
        print(f"   请安装: pip install playwright")
        print(f"   然后运行: playwright install chromium")
        return False
    except Exception as e:
        print(f"❌ 转换失败: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="HTML转长图工具 (Playwright)")
    parser.add_argument("html_file", help="HTML文件路径")
    parser.add_argument("--out", "-o", help="输出PNG文件路径")
    parser.add_argument("--width", "-w", type=int, default=1080, help="图片宽度 (默认: 1080)")
    parser.add_argument("--quality", "-q", type=int, default=100, help="图片质量 0-100 (默认: 100)")
    
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
    success = asyncio.run(html_to_long_png_playwright(
        html_path=html_path,
        output_path=output_path,
        width=args.width,
        quality=args.quality
    ))
    
    if success:
        print(f"\n🎉 长图生成完成!")
        print(f"📱 适合分享到朋友圈/微信群")
        sys.exit(0)
    else:
        print(f"\n❌ 长图生成失败")
        sys.exit(1)

if __name__ == "__main__":
    main()
