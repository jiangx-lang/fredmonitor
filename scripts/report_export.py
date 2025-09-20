#!/usr/bin/env python3
"""
报告导出模块 - 自包含HTML + 朋友圈长图PNG
"""

import re
import base64
import pathlib
from typing import Optional

try:
    from markdown import markdown
except ImportError:
    print("⚠️ 需要安装 markdown: pip install markdown")
    markdown = None

_HTML_SHELL = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{title}</title>
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;line-height:1.6;padding:16px;max-width:940px;margin:auto;background:#fff;color:#111}
  img{max-width:100%;height:auto;display:block;margin:8px 0;}
  code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;}
  h1,h2,h3{line-height:1.25;margin:1.2em 0 .6em}
  .kpi{display:flex;gap:12px;flex-wrap:wrap}.kpi div{padding:8px 12px;border-radius:10px;background:#f5f5f7}
</style>
</head>
<body><article>
{body}
</article></body></html>
"""


def md_to_html(md_text: str, title: str = "宏观金融危机监察报告") -> str:
    """将Markdown转换为HTML"""
    if markdown is None:
        raise RuntimeError("需要安装 markdown: pip install markdown")
    
    body = markdown(md_text, extensions=["tables", "fenced_code"])
    return _HTML_SHELL.format(title=title, body=body)


def inline_local_images(html: str, base_dir: pathlib.Path) -> str:
    """把 <img src="相对路径.png"> 转成 base64，手机端离线可读"""
    def _repl(m):
        src = m.group(1)
        if src.startswith("data:") or src.startswith("http"):
            return m.group(0)
        
        # 尝试直接路径
        p = (base_dir / src).resolve()
        if not p.exists():
            # 报告里常见相对路径: outputs/crisis_monitor/figures/xxx.png
            p2 = (base_dir / "outputs" / "crisis_monitor" / src).resolve()
            if p2.exists():
                p = p2
            else:
                return m.group(0)
        
        # 确定MIME类型
        mime = "image/png" if p.suffix.lower() == ".png" else "image/jpeg"
        
        try:
            b64 = base64.b64encode(p.read_bytes()).decode("ascii")
            return f'<img src="data:{mime};base64,{b64}"'
        except Exception:
            return m.group(0)
    
    return re.sub(r'<img\s+src="([^"]+)"', _repl, html)


def save_html(html: str, out_path: pathlib.Path):
    """保存HTML文件"""
    out_path.write_text(html, encoding="utf-8")


def html_to_png(html_path: pathlib.Path, png_path: pathlib.Path, width: int = 1080):
    """将HTML转换为PNG长图"""
    # 首选 Playwright；如未安装，会给出友好提示
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as e:
        raise RuntimeError("需要安装 Playwright：pip install playwright && playwright install chromium") from e
    
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": width, "height": 1})
        page.goto(f"file://{html_path}", wait_until="networkidle")
        page.add_style_tag(content="body{background:#fff}")
        page.screenshot(path=str(png_path), full_page=True, type="png")
        browser.close()


def export_report(md_path: pathlib.Path, output_dir: pathlib.Path, timestamp: str):
    """导出报告的主要函数"""
    try:
        # 读取Markdown
        html_raw = md_path.read_text(encoding="utf-8")
        
        # 转换为HTML
        html = md_to_html(html_raw, title="宏观金融危机监察报告")
        
        # 内嵌图片
        html = inline_local_images(html, base_dir=output_dir.parent)
        
        # 保存自包含HTML
        html_path = output_dir / f"crisis_report_{timestamp}.html"
        save_html(html, html_path)
        print(f"  🌐 自包含HTML: {html_path}")
        
        # 生成朋友圈长图PNG
        png_path = output_dir / f"crisis_report_{timestamp}_share_1080w.png"
        try:
            html_to_png(html_path, png_path, width=1080)
            print(f"  🖼  PNG长图: {png_path}")
        except Exception as e:
            print(f"  ⚠️ PNG 导出失败（可选）：{e}")
            
    except Exception as e:
        print(f"  ⚠️ HTML/PNG 导出模块异常（可选）：{e}")


if __name__ == "__main__":
    # 测试功能
    import sys
    if len(sys.argv) > 1:
        md_file = pathlib.Path(sys.argv[1])
        if md_file.exists():
            export_report(md_file, md_file.parent, "test")
        else:
            print(f"文件不存在: {md_file}")
    else:
        print("用法: python scripts/report_export.py <markdown_file>")
