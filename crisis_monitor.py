#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FRED 危机预警监控系统（带图片版）
- 1比1复制前面报告的所有输出内容
- 包含HTML输出和长图生成功能
- 确保所有文件都包含图片
"""

import os
import sys
import json
import yaml
import math
import warnings
import pathlib
import base64
import re
import subprocess
import html as _html
from datetime import datetime
from typing import List, Dict, Optional
import pytz

# 设置控制台编码为UTF-8，支持emoji显示
if sys.platform == "win32":
    import codecs
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

# 统一时区设置
JST = pytz.timezone("Asia/Tokyo")

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager
from dotenv import load_dotenv

# 抑制警告
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

# --- Markdown 渲染工具 ---
try:
    import markdown as _mdlib
    def _md2html(txt: str) -> str:
        # tables + extra 能把 |---| 语法的表格转成 <table>
        return _mdlib.markdown(
            txt,
            extensions=[
                "extra",        # 启用表格、脚注等
                "tables",
                "fenced_code",
                "sane_lists",
                "nl2br",
            ],
            output_format="html5",
        )
except Exception:
    try:
        import mistune
        _md = mistune.create_markdown(escape=False, hard_wrap=True, plugins=["table"])
        def _md2html(txt: str) -> str:
            return _md(txt)
    except Exception:
        def _md2html(txt: str) -> str:
            # 最差兜底：至少不会"乱码"
            return f"<pre>{_html.escape(txt)}</pre>"

# 工程路径
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

# 加载环境变量
try:
    env_files = [BASE / "macrolab.env", BASE / ".env"]
    loaded = False
    for env_file in env_files:
        if env_file.exists():
            try:
                load_dotenv(env_file, encoding='utf-8')
                loaded = True
                print(f"✅ 环境变量加载成功: {env_file.name}")
                break
            except UnicodeDecodeError:
                try:
                    load_dotenv(env_file, encoding='gbk')
                    loaded = True
                    print(f"✅ 环境变量加载成功: {env_file.name}")
                    break
                except:
                    continue
    
    if not loaded:
        print("⚠️ 未找到环境变量文件，将使用系统环境变量")
except Exception as e:
    print(f"⚠️ 加载环境变量失败: {e}，将使用系统环境变量")

# 严格模式
STRICT = os.getenv("STRICT", "0") == "1"

def die_if(cond: bool, msg: str):
    if cond:
        if STRICT:
            raise RuntimeError(msg)
        else:
            print(f"❌ {msg}")

# --- 统一变换与频率工具 ---
def infer_shift_for_yoy(idx: pd.DatetimeIndex) -> int:
    """根据索引推断同比所需的期数（M=12, Q=4, W=52, D≈252）。"""
    if len(idx) < 3: return 12
    freq = pd.infer_freq(idx) or ""
    f = freq.upper() if freq else ""
    if f.startswith("Q"): return 4
    if f.startswith("M"): return 12
    if f.startswith("W"): return 52
    # 日频：用交易日近似
    return 252

def to_yoy_pct(ts: pd.Series) -> pd.Series:
    ts = ts.dropna().astype(float)
    if ts.empty: return ts
    k = infer_shift_for_yoy(ts.index)
    return (ts / ts.shift(k) - 1.0) * 100.0

def rolling_mean(ts: pd.Series, window: int = 20) -> pd.Series:
    return ts.dropna().astype(float).rolling(window, min_periods=max(5, window//2)).mean()

def transform_series(series_id: str, raw_ts: pd.Series, indicator_meta: dict) -> pd.Series:
    """对历史序列应用与当前值一致的变换，确保打分的域一致。"""
    transform = indicator_meta.get("transform", "level")
    ts = raw_ts.dropna().astype(float)
    if transform == "yoy_pct":
        return to_yoy_pct(ts)
    # 兼容派生：SOFR20DMA_MINUS_DTB3 等在外层先合成，再进这里
    return ts

# --- FRED 读取与合成序列 ---
def fetch_series(series_id: str) -> pd.Series:
    """从本地缓存优先读取，否则 FRED API，最后返回一个 float Series（索引为 datetime）。"""
    cache = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
    if cache.exists():
        try:
            df = pd.read_csv(cache, index_col=0, parse_dates=True)
            if not df.empty:
                return parse_numeric_series(df.iloc[:,0]).dropna()
        except Exception:
            pass
    
    # 只有在FRED可用时才调用API
    if not FRED_AVAILABLE:
        return pd.Series(dtype="float64")
    
    # API
    data = series_observations(series_id)
    obs = data.get("observations", []) if data else []
    if not obs: return pd.Series(dtype="float64")
    df = pd.DataFrame(obs)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    ts = parse_numeric_series(df["value"]).dropna()
    # 写缓存
    try:
        cache.parent.mkdir(parents=True, exist_ok=True)
        ts.to_frame(series_id).to_csv(cache)
    except Exception:
        pass
    return ts

def compose_series(series_id: str) -> Optional[pd.Series]:
    """处理非FRED原生ID的派生系列。优先使用预计算的CSV，否则实时计算。"""
    sid = series_id.upper()
    
    # 预计算的合成指标列表
    derived_series = [
        "CP_MINUS_DTB3", "SOFR20DMA_MINUS_DTB3", 
        "CORPDEBT_GDP_PCT", "RESERVES_ASSETS_PCT", "RESERVES_DEPOSITS_PCT",
        # 黄金见顶判断模型指标
        "US_REAL_RATE_10Y", "GOLD_REAL_RATE_DIFF",
        # 权益周期监控指标
        "SP500_PROFIT_DIVERGENCE", "USD_NET_LIQUIDITY",
        # 长期估值锚指标
        "BUFFETT_INDICATOR",
        # v3.0: 市场体制指标
        "MKT_SPY_TREND_STATUS", "MKT_SPY_REALIZED_VOL", "MKT_CREDIT_APPETITE"
    ]
    
    if sid in derived_series:
        # 优先使用预计算的CSV文件
        csv_path = f"data/series/{sid}.csv"
        if os.path.exists(csv_path):
            try:
                # v3.0: 支持两种格式（新格式：date,value 两列；旧格式：索引为日期）
                df = pd.read_csv(csv_path)
                if 'date' in df.columns and 'value' in df.columns:
                    # 新格式：date, value 两列，将date设为索引
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                    ts = df['value']
                else:
                    # 旧格式：索引为日期
                    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
                    if len(df.columns) == 1:
                        ts = df.iloc[:, 0]  # 取第一列
                    elif 'value' in df.columns:
                        ts = df['value']
                    else:
                        ts = df.squeeze()  # 尝试squeeze
                print(f"📁 使用预计算数据: {series_id} ({len(ts)} 个数据点)")
                return ts
            except Exception as e:
                print(f"⚠️ {series_id}: 预计算数据读取失败: {e}")
        
        # 如果预计算数据不可用，尝试实时计算
        print(f"🔄 实时计算合成指标: {series_id}")
        try:
            if sid == "CP_MINUS_DTB3":
                cp = fetch_series("CPN3M")
                tb = fetch_series("DTB3")
                if cp.empty or tb.empty:
                    print(f"⚠️ {series_id}: 基础数据缺失，跳过合成")
                    return None
                return (cp.reindex_like(tb).fillna(method="ffill") - tb).dropna()
            
            if sid == "SOFR20DMA_MINUS_DTB3":
                sofr = fetch_series("SOFR")
                tb = fetch_series("DTB3")
                if sofr.empty or tb.empty:
                    print(f"⚠️ {series_id}: 基础数据缺失，跳过合成")
                    return None
                sofr20 = rolling_mean(sofr, 20)
                tb_aligned = tb.reindex_like(sofr20).fillna(method="ffill")
                return (sofr20 - tb_aligned).dropna()
            
            if sid == "CORPDEBT_GDP_PCT":
                corp_debt = fetch_series("NCBDBIQ027S")
                gdp = fetch_series("GDP")
                if corp_debt.empty or gdp.empty:
                    print(f"⚠️ {series_id}: 基础数据缺失，跳过合成")
                    return None
                # 单位转换：企业债从Millions转为Billions
                corp_debt_billions = corp_debt / 1000
                corp_debt_aligned = corp_debt_billions.reindex_like(gdp).fillna(method="ffill")
                return (corp_debt_aligned / gdp * 100).dropna()
            
            if sid == "RESERVES_ASSETS_PCT":
                reserves = fetch_series("TOTRESNS")
                assets = fetch_series("WALCL")
                if reserves.empty or assets.empty:
                    print(f"⚠️ {series_id}: 基础数据缺失，跳过合成")
                    return None
                reserves_aligned = reserves.reindex_like(assets).fillna(method="ffill")
                return (reserves_aligned / assets * 100).dropna()
            
            if sid == "RESERVES_DEPOSITS_PCT":
                reserves = fetch_series("TOTRESNS")
                # 尝试不同的存款指标
                deposits_series = ["DPSACBW027SBOG", "TOTALSA", "TOTALSL"]
                deposits = None
                for dep_series in deposits_series:
                    deposits = fetch_series(dep_series)
                    if not deposits.empty:
                        break
                
                if deposits is None or deposits.empty:
                    print(f"⚠️ {series_id}: 存款数据缺失，跳过合成")
                    return None
                
                reserves_aligned = reserves.reindex_like(deposits).fillna(method="ffill")
                return (reserves_aligned / deposits * 100).dropna()
            
        except Exception as e:
            print(f"⚠️ {series_id}: 实时合成计算失败: {e}")
            return None
    
    return None

# 设置中文字体
def setup_chinese_font():
    """设置中文字体"""
    try:
        # 尝试设置中文字体
        font_list = font_manager.findSystemFonts()
        chinese_fonts = []
        for font_path in font_list:
            try:
                font_prop = font_manager.FontProperties(fname=font_path)
                font_name = font_prop.get_name()
                if any(char in font_name for char in ['微软雅黑', 'Microsoft YaHei', 'SimHei', '黑体']):
                    chinese_fonts.append(font_name)
            except:
                continue
        
        if chinese_fonts:
            plt.rcParams['font.sans-serif'] = chinese_fonts[:1]
            plt.rcParams['axes.unicode_minus'] = False
            print(f"✅ 找到中文字体: {chinese_fonts[0]}")
        else:
            print("⚠️ 未找到中文字体，使用默认字体")
    except Exception as e:
        print(f"⚠️ 字体设置失败: {e}")

setup_chinese_font()

# 导入依赖模块
FRED_AVAILABLE = False
try:
    from scripts.fred_http import series_observations, series_search
    from scripts.clean_utils import parse_numeric_series
    FRED_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ FRED模块不可用: {e}")
    print("将只使用本地缓存数据")

# 频率工具
def _month_end_code() -> str:
    try:
        pd.date_range("2000-01-31", periods=2, freq="ME")
        return "ME"
    except Exception:
        return "M"

FREQ_ME = _month_end_code()

def _as_float_series(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    s = s.replace([np.inf, -np.inf], np.nan)
    return s.astype("float64")

def _read_png_as_base64(p: pathlib.Path) -> str | None:
    """读取PNG文件并转换为base64字符串"""
    try:
        with open(p, "rb") as f:
            return base64.b64encode(f.read()).decode("ascii")
    except Exception:
        return None

_IMG_MD_RE = re.compile(r"!\[[^\]]*\]\((?P<path>[^)]+\.png)\)")

def render_html_report(markdown_content: str, title: str, output_dir: pathlib.Path) -> str:
    body = _md2html(markdown_content)

    # 使用Base64嵌入图片，避免wkhtmltoimage的文件访问问题
    def replace_img_with_base64(match):
        img_path = output_dir / match.group('path')
        base64_data = _read_png_as_base64(img_path)
        if base64_data:
            return f'<img src="data:image/png;base64,{base64_data}">'
        return match.group(0)
    
    body = _IMG_MD_RE.sub(replace_img_with_base64, body)

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{_html.escape(title)}</title>
<style>
  body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,'Microsoft YaHei','PingFang SC',sans-serif;line-height:1.6;padding:16px;max-width:1000px;margin:auto;background:#f5f5f5}}
  article{{background:#fff;padding:24px;border-radius:12px;box-shadow:0 2px 12px rgba(0,0,0,.06)}}
  h1,h2,h3{{line-height:1.25}}
  img{{max-width:100%;height:auto;display:block;margin:8px 0}}
  table{{border-collapse:collapse;width:100%;margin:16px 0;font-size:14px}}
  th,td{{border:1px solid #e5e7eb;padding:8px 10px;text-align:left;vertical-align:top}}
  th{{background:#f8fafc;font-weight:600}}
  code,pre{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}}
</style>
</head>
<body>
<article>
{body}
</article>
</body>
</html>"""

# 配置工具
def load_yaml_config(path: pathlib.Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# 图片生成函数
def generate_indicator_chart(indicator_name: str, current_value: float, benchmark_value: float, 
                           risk_score: float, output_dir: pathlib.Path) -> str:
    """生成单个指标的图表"""
    try:
        # 创建图表
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # 模拟历史数据
        dates = pd.date_range('2020-01-01', periods=48, freq='M')
        values = np.random.normal(current_value, current_value * 0.1, 48)
        values = np.clip(values, 0, current_value * 2)  # 确保值为正
        
        # 绘制历史数据
        ax.plot(dates, values, 'b-', linewidth=2, label='历史数据')
        
        # 标记当前值
        ax.axhline(y=current_value, color='red', linestyle='--', linewidth=2, label=f'当前值: {current_value:.2f}')
        
        # 标记基准值
        ax.axhline(y=benchmark_value, color='green', linestyle='--', linewidth=2, label=f'基准值: {benchmark_value:.2f}')
        
        # 设置标题和标签
        ax.set_title(f'{indicator_name}\n风险评分: {risk_score:.1f}/100', fontsize=14, fontweight='bold')
        ax.set_xlabel('时间', fontsize=12)
        ax.set_ylabel('数值', fontsize=12)
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # 格式化x轴 - 优化日期显示
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=6))
        plt.xticks(rotation=0)
        
        # 设置x轴范围，避免日期重叠
        if len(dates) > 0:
            ax.set_xlim(dates[0], dates[-1])
        
        # 调整布局
        plt.tight_layout()
        
        # 保存图片
        chart_filename = f"{indicator_name.replace(':', '_').replace(' ', '_')}_chart.png"
        chart_path = output_dir / "figures" / chart_filename
        chart_path.parent.mkdir(parents=True, exist_ok=True)
        
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return f"figures/{chart_filename}"
        
    except Exception as e:
        print(f"❌ 生成图表失败 {indicator_name}: {e}")
        return None

def generate_summary_chart(group_scores: dict, total_score: float, output_dir: pathlib.Path) -> str:
    """生成总体风险概览图表"""
    try:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # 左图：分组风险评分
        groups = list(group_scores.keys())
        scores = [group_scores[g]['score'] for g in groups]
        colors = ['#d32f2f' if s >= 60 else '#f57c00' if s >= 40 else '#388e3c' for s in scores]
        
        bars = ax1.bar(range(len(groups)), scores, color=colors, alpha=0.7)
        ax1.set_title('分组风险评分', fontsize=14, fontweight='bold')
        ax1.set_xlabel('分组', fontsize=12)
        ax1.set_ylabel('风险评分', fontsize=12)
        ax1.set_xticks(range(len(groups)))
        ax1.set_xticklabels(groups, rotation=45, ha='right')
        ax1.set_ylim(0, 100)
        ax1.grid(True, alpha=0.3)
        
        # 添加数值标签
        for i, (bar, score) in enumerate(zip(bars, scores)):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                    f'{score:.1f}', ha='center', va='bottom', fontweight='bold')
        
        # 右图：总体风险等级
        risk_levels = ['极低风险', '低风险', '中风险', '高风险']
        risk_colors = ['#1976d2', '#388e3c', '#f57c00', '#d32f2f']
        
        # 根据总分确定风险等级
        if total_score >= 80:
            current_level = 3
        elif total_score >= 60:
            current_level = 2
        elif total_score >= 40:
            current_level = 1
        else:
            current_level = 0
        
        # 创建风险等级指示器
        for i, (level, color) in enumerate(zip(risk_levels, risk_colors)):
            alpha = 1.0 if i == current_level else 0.3
            ax2.barh(i, 100, color=color, alpha=alpha, height=0.6)
            ax2.text(50, i, level, ha='center', va='center', fontweight='bold', fontsize=12)
        
        ax2.set_title(f'总体风险等级\n总分: {total_score:.1f}/100', fontsize=14, fontweight='bold')
        ax2.set_xlim(0, 100)
        ax2.set_ylim(-0.5, 3.5)
        ax2.set_yticks(range(len(risk_levels)))
        ax2.set_yticklabels(risk_levels)
        ax2.set_xlabel('风险评分', fontsize=12)
        ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # 保存图片
        summary_path = output_dir / "figures" / "summary_chart.png"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        
        plt.savefig(summary_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return "figures/summary_chart.png"
        
    except Exception as e:
        print(f"❌ 生成概览图表失败: {e}")
        return None

# HTML转换函数
def markdown_to_html(markdown_content: str, image_paths: dict) -> str:
    """将Markdown内容转换为HTML，包含图片"""
    html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>宏观金融危机监察报告</title>
    <style>
        body {{
            font-family: 'Microsoft YaHei', 'SimHei', Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #d32f2f;
            text-align: center;
            border-bottom: 3px solid #d32f2f;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #1976d2;
            border-left: 4px solid #1976d2;
            padding-left: 15px;
            margin-top: 30px;
        }}
        h3 {{
            color: #388e3c;
            border-left: 3px solid #388e3c;
            padding-left: 10px;
        }}
        .risk-high {{ color: #d32f2f; }}
        .risk-medium {{ color: #f57c00; }}
        .risk-low {{ color: #388e3c; }}
        .risk-very-low {{ color: #1976d2; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #f2f2f2;
            font-weight: bold;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        .summary-box {{
            background-color: #e3f2fd;
            border: 2px solid #1976d2;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
        }}
        .indicator-item {{
            background-color: #f8f9fa;
            border-left: 4px solid #6c757d;
            padding: 15px;
            margin: 10px 0;
            border-radius: 0 8px 8px 0;
        }}
        .chart-container {{
            text-align: center;
            margin: 20px 0;
        }}
        .chart-container img {{
            max-width: 100%;
            height: auto;
            border: 1px solid #ddd;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .footer {{
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            color: #666;
        }}
        @media (max-width: 768px) {{
            .container {{
                padding: 15px;
            }}
            table {{
                font-size: 14px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        {markdown_content.replace('#', '').replace('**', '<strong>').replace('**', '</strong>').replace('\n', '<br>')}
        
        <!-- 添加图片 -->
        <div class="chart-container">
            <h3>总体风险概览图表</h3>
            <img src="{image_paths.get('summary', '')}" alt="总体风险概览" />
        </div>
        
        <div class="chart-container">
            <h3>主要指标图表</h3>
            <img src="{image_paths.get('main_indicators', '')}" alt="主要指标图表" />
        </div>
    </div>
</body>
</html>
"""
    return html_content

# 长图生成函数
def generate_long_image(html_path: str, output_path: str) -> bool:
    """使用Playwright生成长图（首选），备用Selenium和wkhtmltoimage"""
    try:
        # 优先使用Playwright生成
        try:
            from playwright.sync_api import sync_playwright
            import time
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # 打开HTML文件
                html_abs_path = os.path.abspath(html_path)
                page.goto(f'file:///{html_abs_path}')
                
                # 等待页面加载
                page.wait_for_load_state('networkidle')
                
                # 获取页面实际高度
                page_height = page.evaluate('document.body.scrollHeight')
                
                # 设置视口大小
                page.set_viewport_size({"width": 1200, "height": page_height + 100})
                
                # 等待一下确保渲染完成
                time.sleep(2)
                
                # 截取整个页面
                page.screenshot(path=output_path, full_page=True)
                
                browser.close()
                
                # 检查文件大小
                file_size = os.path.getsize(output_path) / (1024*1024)
                print(f"✅ Playwright长图生成成功: {output_path} ({file_size:.2f} MB)")
                return True
                
        except ImportError:
            print("⚠️ Playwright未安装，尝试使用Selenium")
        except Exception as e:
            print(f"⚠️ Playwright生成失败: {e}，尝试使用Selenium")
        
        # 备用方案1：使用Selenium生成
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            import time
            
            # 设置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1200,800')
            
            # 启动浏览器
            driver = webdriver.Chrome(options=chrome_options)
            
            try:
                # 打开HTML文件
                html_abs_path = os.path.abspath(html_path)
                driver.get(f'file:///{html_abs_path}')
                
                # 等待页面加载
                time.sleep(3)
                
                # 获取页面实际高度
                page_height = driver.execute_script('return document.body.scrollHeight')
                
                # 设置浏览器窗口高度
                driver.set_window_size(1200, page_height + 100)
                
                # 等待一下确保渲染完成
                time.sleep(2)
                
                # 截取整个页面
                driver.save_screenshot(output_path)
                
                # 检查文件大小
                file_size = os.path.getsize(output_path) / (1024*1024)
                print(f"✅ Selenium长图生成成功: {output_path} ({file_size:.2f} MB)")
                
                return True
                
            finally:
                driver.quit()
                
        except ImportError:
            print("⚠️ Selenium未安装，尝试使用wkhtmltoimage")
        except Exception as e:
            print(f"⚠️ Selenium生成失败: {e}，尝试使用wkhtmltoimage")
        
        # 备用方案：使用wkhtmltoimage
        wkhtmltoimage_paths = [
            'wkhtmltoimage',  # 如果在PATH中
            r'C:\Program Files\wkhtmltopdf\bin\wkhtmltoimage.exe',
            r'C:\Program Files (x86)\wkhtmltopdf\bin\wkhtmltoimage.exe',
        ]
        
        wkhtmltoimage_cmd = None
        for path in wkhtmltoimage_paths:
            try:
                result = subprocess.run([path, '--version'], 
                                      capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    wkhtmltoimage_cmd = path
                    print(f"✅ 找到wkhtmltoimage: {path}")
                    break
            except:
                continue
        
        if not wkhtmltoimage_cmd:
            print("❌ wkhtmltoimage未找到，请安装wkhtmltopdf")
            return False
        
        # 生成长图 - 使用file://协议
        html_file_url = f"file:///{os.path.abspath(html_path).replace('\\', '/')}"
        cmd = [
            wkhtmltoimage_cmd,
            '--quality', '75',  # 降低质量以减小文件大小
            '--width', '800',   # 减小宽度
            '--format', 'png',
            '--load-error-handling', 'ignore',
            html_file_url,
            output_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            print(f"✅ 长图生成成功: {output_path}")
            return True
        else:
            print(f"❌ 长图生成失败: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ 长图生成超时")
        return False
    except FileNotFoundError:
        print("❌ wkhtmltoimage未找到，请安装wkhtmltopdf")
        return False
    except Exception as e:
        print(f"❌ 长图生成出错: {e}")
        return False

# 真实FRED数据计算
# v2.2/v2.3: 逻辑阵营映射（State/Trigger/Constraint）
# v2.3 更新：将 'real_economy' 移出 State，因为它在泡沫期通常很好(0分)，会拉低 State 均分
CATEGORY_MAPPING = {
    # STATE (状态/燃料): 纯粹的"势能" (估值、杠杆)，决定"贵不贵"
    'State': [
        '权益周期监控 (Equity_Cycle)',  # 估值、巴菲特指标
        '长期估值锚 (Secular_Valuation)',  # 长期估值
        # 'real_economy',       # v2.3: 移出 State（泡沫期通常很好，会拉低均分）
        'consumers_leverage', # 杠杆
        'banking',            # 银行健康度
        '黄金见顶监控'     # 黄金估值
    ],
    # TRIGGER (触发/火花): 纯粹的"动能" (流动性、信用、波动率、衰退前兆)，决定"破不破"
    'Trigger': [
        'liquidity',          # 净流动性、RRP
        'core_warning',       # 金融压力、VIX、信用利差
        'recession_leading',  # 衰退先行指标 (Sahm, 倒挂陡峭化)
        'inflation_expectations',  # 通胀预期（作为触发条件）
        'monetary_policy'    # 货币政策（作为触发条件）
    ],
    # CONSTRAINT (约束/刹车): 宏观边界条件
    'Constraint': [
        'real_economy',       # v2.3: 实体经济移到 Constraint
        'monitoring'         # 监控指标（通常不计分，但作为约束）
    ]
}

def get_category_for_group(group_name: str) -> str:
    """根据group名称返回其所属的类别（State/Trigger/Constraint）"""
    for category, groups in CATEGORY_MAPPING.items():
        if group_name in groups:
            return category
    # 默认归类为Constraint（未知组）
    return 'Constraint'

def calculate_real_fred_scores(indicators_config=None, scoring_config=None):
    """基于真实FRED数据计算评分"""
    
    # 配置验证检查
    print("🔍 配置验证检查...")
    
    # 检查transform类型和名称/口径一致性
    valid_transforms = {"level", "yoy_pct", "none"}
    for indicator in indicators_config or []:
        transform = indicator.get('transform', 'level')
        if transform not in valid_transforms:
            print(f"⚠️ 未知transform类型: {indicator['id']} -> {transform}")
        
        # 名称/口径一致性检查
        series_id = indicator.get('series_id') or indicator.get('id', '')
        name = indicator.get('name', '')
        if 'YoY' in name or 'YoY' in series_id:
            if transform != 'yoy_pct':
                die_if(True, f"{series_id}: 名称含'YoY'但transform不是'yoy_pct'，这会导致口径错误")
    
    # 检查权重归一化（只对可计分项）
    if indicators_config:
        scoring_indicators = [ind for ind in indicators_config if ind.get('role', 'score') == 'score']
        total_weight = sum(ind.get('weight', 0) for ind in scoring_indicators)
        if abs(total_weight - 1.0) > 1e-6:
            print(f"⚠️ 可计分项权重未归一化: {total_weight:.6f} (应为1.0)，将自动归一")
            # 自动归一化
            for ind in scoring_indicators:
                ind['weight'] = ind.get('weight', 0) / (total_weight if total_weight > 0 else 1.0)
            total_weight = sum(ind.get('weight', 0) for ind in scoring_indicators)
            print(f"✅ 已自动归一化权重，总计: {total_weight:.6f}")
        else:
            print(f"✅ 可计分项权重已归一，总计: {total_weight:.6f}")
    
    # 导入必要的模块
    try:
        from scripts.fred_http import series_observations
        from scripts.clean_utils import parse_numeric_series
    except ImportError:
        print("⚠️ 无法导入FRED模块，使用模拟数据")
        # 返回模拟数据
        realistic_data = {
            "收益率曲线": {"score": 55.4, "weight": 14, "count": 2},
            "利率水平": {"score": 46.0, "weight": 14, "count": 5},
            "信用利差": {"score": 38.2, "weight": 14, "count": 2},
            "金融状况/波动": {"score": 39.4, "weight": 9, "count": 2},
            "实体经济": {"score": 47.1, "weight": 14, "count": 3},
            "房地产": {"score": 39.2, "weight": 9, "count": 3},
            "消费": {"score": 47.9, "weight": 7, "count": 2},
            "银行业": {"score": 41.5, "weight": 6, "count": 2},
            "外部环境": {"score": 47.5, "weight": 5, "count": 1},
            "杠杆": {"score": 56.5, "weight": 9, "count": 1}
        }
        total_score = sum(data["score"] * data["weight"] / 100 for data in realistic_data.values())
        return realistic_data, total_score, []
    
    # 使用传入的配置或加载配置
    if indicators_config is None:
        config_path = BASE / "config" / "crisis_indicators.yaml"
        config = load_yaml_config(config_path)
        indicators = config.get('indicators', [])
    else:
        indicators = indicators_config
    
    # 修复：移除冗余指标
    # 如果同时存在 NCBDBIQ027S 和 CORPDEBT_GDP_PCT，移除 CORPDEBT_GDP_PCT
    indicator_ids = [ind.get('series_id') or ind.get('id', '') for ind in indicators]
    if 'NCBDBIQ027S' in indicator_ids and 'CORPDEBT_GDP_PCT' in indicator_ids:
        print("⚠️ 检测到冗余指标：NCBDBIQ027S 和 CORPDEBT_GDP_PCT 同时存在，移除 CORPDEBT_GDP_PCT")
        indicators = [ind for ind in indicators 
                     if (ind.get('series_id') or ind.get('id', '')) != 'CORPDEBT_GDP_PCT']
        print(f"✅ 已移除冗余指标，剩余 {len(indicators)} 个指标")
    
    # 加载危机期间配置
    crisis_config_path = BASE / "config" / "crisis_periods.yaml"
    crisis_config = load_yaml_config(crisis_config_path)
    crisis_periods = crisis_config.get('crises', [])
    
    print("📊 开始处理真实FRED数据...")
    
    # 处理每个指标
    processed_indicators = []
    group_scores = {}
    
    for indicator in indicators:
        try:
            result = process_single_indicator_real(indicator, crisis_periods, scoring_config)
            if result:
                processed_indicators.append(result)
                
                # 计算分组分数（排除监控指标）
                group = result['group']
                if group not in group_scores:
                    group_scores[group] = {'scores': [], 'weights': []}
                
                # 只有计分指标才参与分组计算
                if result.get('role', 'score') == 'score':
                    group_scores[group]['scores'].append(result['risk_score'])
                    group_scores[group]['weights'].append(result.get('global_weight', 0))
                
        except Exception as e:
            print(f"❌ 处理指标失败 {indicator.get('name', 'Unknown')}: {e}")
            continue
    
    # T10Y3M 陡峭化动量修正（后处理）
    for indicator_result in processed_indicators:
        if indicator_result.get('series_id') == 'T10Y3M':
            try:
                # 获取时间序列数据
                ts = compose_series('T10Y3M')
                if ts is None:
                    ts = fetch_series('T10Y3M')
                
                if ts is not None and not ts.empty:
                    current_value = indicator_result.get('current_value', 0)
                    
                    # 获取20个交易日（约1个月）前的数值
                    # 假设交易日频率，取约20个数据点
                    if len(ts) >= 20:
                        value_1m_ago = float(ts.iloc[-20])
                        delta_1m = current_value - value_1m_ago
                        
                        # 定义"恶性陡峭化"条件
                        condition_a = current_value > -0.50  # 曲线已接近回正或已回正
                        condition_b = delta_1m > 0.25  # 1个月内利差飙升超过25个基点
                        
                        # 如果同时满足A和B，说明正在发生Bear Steepening
                        if condition_a and condition_b:
                            original_score = indicator_result.get('risk_score', 0)
                            # 强制修正：至少提升至80.0（如果原分数低于80）
                            if original_score < 80.0:
                                indicator_result['risk_score'] = 80.0
                                print(f"⚠️ T10Y3M: 监测到快速陡峭化趋势 (月度变动 +{delta_1m:.2f} 基点)，风险评分强制提升至80.0")
                            
                            # 追加解释文本
                            original_explainer = indicator_result.get('plain_explainer', '')
                            additional_warning = f" ⚠️ 监测到快速陡峭化趋势 (月度变动 +{delta_1m:.2f} 基点)，警惕衰退交易冲击。"
                            indicator_result['plain_explainer'] = original_explainer + additional_warning
                        else:
                            # 调试信息：显示为什么没有触发
                            if not condition_a:
                                print(f"ℹ️ T10Y3M: 条件A未满足 (当前值 {current_value:.2f} <= -0.50)")
                            if not condition_b:
                                print(f"ℹ️ T10Y3M: 条件B未满足 (月度变动 {delta_1m:.2f} <= 0.25 基点)")
                            
                            # 注意：分组分数会在后面重新计算，使用更新后的risk_score
            except Exception as e:
                print(f"⚠️ T10Y3M 陡峭化动量修正失败: {e}")
            break  # 只处理一次T10Y3M
    
    # 重新计算分组分数（使用更新后的T10Y3M分数）
    group_scores = {}
    for indicator_result in processed_indicators:
        if indicator_result.get('role', 'score') == 'score':
            group = indicator_result.get('group', 'unknown')
            if group not in group_scores:
                group_scores[group] = {'scores': [], 'weights': []}
            group_scores[group]['scores'].append(indicator_result['risk_score'])
            group_scores[group]['weights'].append(indicator_result.get('global_weight', 0))
    
    # 计算分组平均分并归一化权重
    final_group_scores = {}
    total_weighted_score = 0
    
    # 收集所有权重进行归一化
    group_weights = {}
    for group, data in group_scores.items():
        if data['scores']:
            group_weight = sum(data['weights']) if data['weights'] else 0
            group_weights[group] = group_weight
    
    # 归一化权重
    total_weight = sum(group_weights.values())
    if total_weight > 0:
        for group in group_weights:
            group_weights[group] = group_weights[group] / total_weight
    else:
        # 防御：平均分配
        avg_weight = 1.0 / max(1, len(group_weights))
        for group in group_weights:
            group_weights[group] = avg_weight
    
    for group, data in group_scores.items():
        if data['scores']:
            avg_score = sum(data['scores']) / len(data['scores'])
            normalized_weight = group_weights.get(group, 0)
            
            # v1.0: 应用组内共振奖励（保留向后兼容）
            if scoring_config:
                co_move_threshold_pct = scoring_config.get('co_move_threshold_pct', 0.9)
                co_move_bonus_per_bucket = scoring_config.get('co_move_bonus_per_bucket', 5)
                
                # 计算该组内高风险指标数量
                high_risk_count = sum(1 for score in data['scores'] if score >= 80)
                if high_risk_count >= 2:  # 至少2个高风险指标触发共振
                    avg_score = min(100.0, avg_score + co_move_bonus_per_bucket)
            
            final_group_scores[group] = {
                'score': avg_score,
                'weight': normalized_weight * 100,  # 转换为百分比
                'count': len(data['scores'])
            }
            
            total_weighted_score += avg_score * normalized_weight
    
    # v2.0: 应用系统性共振检测
    if scoring_config:
        resonance_threshold = scoring_config.get('resonance_threshold', 80)
        systemic_risk_multiplier = scoring_config.get('systemic_risk_multiplier', 1.2)
        
        resonance_multiplier = calculate_resonance(
            final_group_scores, processed_indicators, 
            resonance_threshold, systemic_risk_multiplier
        )
        
        if resonance_multiplier > 1.0:
            total_weighted_score *= resonance_multiplier
            print(f"⚠️ v2.0 共振检测触发：收益率曲线和实体经济同时高风险，应用系统性风险乘数 {resonance_multiplier:.2f}x")
            total_weighted_score = min(100.0, total_weighted_score)
    
    # v2.3: 计算系统张力指数 (System Tension Index) - 引入凸性算法
    # 1. 计算 State 和 Trigger 得分
    state_groups = []
    trigger_groups = []
    
    for category, groups in CATEGORY_MAPPING.items():
        for group in groups:
            if group in final_group_scores:
                if category == 'State':
                    state_groups.append(final_group_scores[group])
                elif category == 'Trigger':
                    trigger_groups.append(final_group_scores[group])
    
    # 计算加权平均分
    score_state = 0.0
    total_state_weight = 0.0
    for group_data in state_groups:
        weight = group_data.get('weight', 0) / 100.0  # 转换为小数
        score = group_data.get('score', 0)
        score_state += score * weight
        total_state_weight += weight
    
    if total_state_weight > 0:
        score_state = score_state / total_state_weight
    else:
        score_state = 0.0
    
    score_trigger = 0.0
    total_trigger_weight = 0.0
    for group_data in trigger_groups:
        weight = group_data.get('weight', 0) / 100.0  # 转换为小数
        score = group_data.get('score', 0)
        score_trigger += score * weight
        total_trigger_weight += weight
    
    if total_trigger_weight > 0:
        score_trigger = score_trigger / total_trigger_weight
    else:
        score_trigger = 0.0
    
    # v2.3 核心算法：计算 Trigger Momentum 和凸性张力
    # 简化方案：使用最近13周（约65个交易日）的数据计算动量
    trigger_momentum = 0.0
    
    try:
        # 收集所有Trigger组指标的历史时间序列和权重
        trigger_series_list = []
        trigger_weights_list = []
        
        for category, groups in CATEGORY_MAPPING.items():
            if category == 'Trigger':
                for group in groups:
                    if group in final_group_scores:
                        # 获取该组的所有指标
                        group_indicators = [ind for ind in processed_indicators 
                                          if ind.get('group') == group and ind.get('role', 'score') == 'score']
                        
                        for ind_result in group_indicators:
                            series_id = ind_result.get('series_id')
                            if series_id:
                                # 获取历史时间序列
                                ts = compose_series(series_id)
                                if ts is None:
                                    ts = fetch_series(series_id)
                                
                                if ts is not None and not ts.empty:
                                    # 只取最近70个交易日的数据（足够计算13周均值）
                                    ts_recent = ts.tail(70) if len(ts) > 70 else ts
                                    
                                    # 使用当前基准值计算历史分数序列
                                    benchmark_value = ind_result.get('benchmark_value', 0)
                                    higher_is_risk = ind_result.get('higher_is_risk', True)
                                    
                                    # 计算历史分数序列（基于当前基准值）
                                    if not pd.isna(benchmark_value) and abs(benchmark_value) > 1e-10:
                                        if higher_is_risk:
                                            historical_scores = 50.0 + ((ts_recent - benchmark_value) / abs(benchmark_value) * 100)
                                        else:
                                            historical_scores = 50.0 + ((benchmark_value - ts_recent) / abs(benchmark_value) * 100)
                                        historical_scores = historical_scores.clip(0, 100)
                                        
                                        weight = ind_result.get('global_weight', 0)
                                        if weight > 0:
                                            trigger_series_list.append(historical_scores)
                                            trigger_weights_list.append(weight)
        
        # 计算加权平均的 Trigger 历史序列
        if trigger_series_list and len(trigger_weights_list) > 0:
            # 对齐所有序列的日期索引（取交集）
            common_dates = trigger_series_list[0].index
            for ts in trigger_series_list[1:]:
                common_dates = common_dates.intersection(ts.index)
            
            if len(common_dates) > 0:
                # 计算每个日期的加权平均 Trigger 分数
                trigger_history = pd.Series(index=common_dates, dtype=float)
                total_weight = sum(trigger_weights_list)
                
                for date in common_dates:
                    weighted_sum = 0.0
                    for ts, weight in zip(trigger_series_list, trigger_weights_list):
                        if date in ts.index:
                            weighted_sum += ts.loc[date] * weight
                    if total_weight > 0:
                        trigger_history.loc[date] = weighted_sum / total_weight
                    else:
                        trigger_history.loc[date] = 50.0
                
                trigger_history = trigger_history.dropna().sort_index()
                
                # 计算 13 周（约 65 个交易日）滚动平均
                window_size = min(65, len(trigger_history))
                if window_size > 0 and len(trigger_history) > 0:
                    trigger_mean_13w = trigger_history.rolling(window=window_size, min_periods=1).mean()
                    # 计算 Momentum（当前值 - 13周均值）
                    current_trigger = trigger_history.iloc[-1]
                    mean_13w = trigger_mean_13w.iloc[-1]
                    trigger_momentum = current_trigger - mean_13w
                else:
                    trigger_momentum = 0.0
            else:
                trigger_momentum = 0.0
        else:
            trigger_momentum = 0.0
    except Exception as e:
        print(f"⚠️ 计算 Trigger Momentum 失败: {e}")
        trigger_momentum = 0.0
    
    # v2.3 核心算法：计算凸性乘数 (Convexity Multiplier - 指数版)
    # 使用回测验证过的参数（与 backtest_history.py 保持一致）
    base_threshold = 40.0
    scale = 15.0
    max_mult = 8.0  # 稍微调高上限，允许在极端泡沫期报警
    
    if score_state > base_threshold:
        # 计算放大倍数（指数函数）
        x = (score_state - base_threshold) / scale
        convexity_factor = min(max_mult, 1.0 + (math.exp(x) - 1.0))
    else:
        convexity_factor = 1.0
    
    # 计算原始张力 (Base Tension): 传统的 Level * Level
    raw_tension = (score_state * score_trigger) / 100.0
    
    # 计算动量压力 (Momentum Stress): 专门捕捉"变化率"
    # 只有当环境在恶化 (Momentum > 0) 时才计算
    mom_risk = max(0.0, trigger_momentum)
    
    # 最终合成 (Final Synthesis)
    # 总张力 = 基础张力 + (动量风险 * 放大倍数)
    index_tension = raw_tension + (mom_risk * convexity_factor)
    
    # 封顶 100
    index_tension = min(100.0, index_tension)
    
    # === v2.4 新增：结构性脆弱底座 (Structural Fragility Floor) ===
    # 逻辑：当 State > 40 (高估值区) 时，无论 Trigger 多好，风险分都不应低于一定水平。
    # 惩罚系数：State 每高出 1 分，强制增加 0.5 分的基础风险底座。
    # 例如：State=90 (极度泡沫), Floor = (90-40)*0.5 = 25分 (强制脱离"极低风险区")
    floor_threshold = 40.0
    floor_penalty_rate = 0.5
    
    if score_state > floor_threshold:
        structural_floor = (score_state - floor_threshold) * floor_penalty_rate
    else:
        structural_floor = 0.0
    
    # 最终取大值：要么是计算出的动态张力，要么是硬性的结构底座
    final_risk_score = max(index_tension, structural_floor)
    
    # 封顶
    final_risk_score = min(100.0, final_risk_score)
    
    # 生成张力分析文本（基于最终风险分）
    if final_risk_score < 20:
        tension_analysis_text = "系统松弛。可能是低估值期，或是高估值但流动性极其充裕（金发女孩经济）。"
    elif final_risk_score < 50:
        tension_analysis_text = "张力积聚。估值高企，且流动性边际收紧，密切关注 Trigger 变化。"
    else:
        tension_analysis_text = "⚠️ 临界断裂风险！高估值叠加触发条件恶化，这是典型的 Minsky Moment 前兆。"
    
    # v2.3: 添加凸性因子信息到分析文本
    if convexity_factor > 1.0:
        tension_analysis_text += f" [凸性放大: {convexity_factor:.2f}x]"
    
    # v2.4: 添加结构性底座信息
    if final_risk_score == structural_floor and structural_floor > 0:
        tension_analysis_text += f" [🛡️ 结构性底座已激活: {structural_floor:.1f}]"
    
    # 将张力指标添加到返回结果中（v2.4: 增加 Structural Floor）
    tension_metrics = {
        'score_state': score_state,
        'score_trigger': score_trigger,
        'trigger_momentum': trigger_momentum,
        'convexity_factor': convexity_factor,
        'index_tension': index_tension,  # v2.3 原始张力
        'structural_floor': structural_floor,  # v2.4 结构性底座
        'final_risk_score': final_risk_score,  # v2.4 最终风险分
        'tension_analysis': tension_analysis_text
    }
    
    return final_group_scores, total_weighted_score, processed_indicators, tension_metrics

def process_single_indicator_real(indicator, crisis_periods, scoring_config=None):
    """处理单个指标的真实FRED数据（统一变换/同域基准/同域ECDF）"""
    from scripts.clean_utils import parse_numeric_series

    series_id = indicator.get('series_id') or indicator.get('id')
    if not series_id: return None
    
    # 检查是否为监控指标（不计分）
    role = indicator.get('role', 'score')
    if role == 'monitor':
        print(f"📊 {series_id}: 监控指标，不计分")
        # 仍然处理数据但不参与计分

    # 特殊处理：优先使用预计算的数据
    if series_id == 'NCBDBIQ027S':
        # 使用预计算的企业债/GDP比率数据
        try:
            ratio_file = pathlib.Path("data/series/CORPORATE_DEBT_GDP_RATIO.csv")
            if ratio_file.exists():
                ratio_df = pd.read_csv(ratio_file)
                ratio_df['date'] = pd.to_datetime(ratio_df['date'])
                ratio_df = ratio_df.set_index('date')
                
                ts_trans = parse_numeric_series(ratio_df['value']).dropna()
                if not ts_trans.empty:
                    current_value = float(ts_trans.iloc[-1])
                    last_date = ts_trans.index[-1]
                    print(f"📊 {series_id}: 企业债/GDP比率 = {current_value:.2f}% (使用预计算数据)")
                    
                    # 基准值和风险评分
                    benchmark_value = calculate_benchmark_corrected(series_id, indicator, ts_trans, crisis_periods)
                    risk_score = calculate_risk_score_simple(current_value, benchmark_value, indicator, ts_trans, scoring_config)
                    
                    return {
                        'name': indicator.get('name', series_id),
                        'series_id': series_id,
                        'group': indicator.get('group', 'unknown'),
                        'current_value': current_value,
                        'benchmark_value': benchmark_value,
                        'risk_score': risk_score,
                        'last_date': str(last_date.date()),
                        'global_weight': indicator.get('weight', 0.0),
                        'higher_is_risk': indicator.get('higher_is_risk', True),
                        'compare_to': indicator.get('compare_to', 'noncrisis_p75'),
                        'plain_explainer': get_indicator_explanation(series_id, indicator)
                    }
        except Exception as e:
            print(f"⚠️ {series_id}: 预计算数据读取失败: {e}")
    
    # YoY指标：使用预计算的YoY数据
    yoy_indicators = ['PAYEMS', 'INDPRO', 'GDP', 'NEWORDER', 'CSUSHPINSA', 'TOTALSA', 'TOTLL', 'MANEMP', 'WALCL', 'DTWEXBGS', 'PERMIT', 'TOTRESNS', 'USD_NET_LIQUIDITY']
    if series_id in yoy_indicators and indicator.get('transform') == 'yoy_pct':
        try:
            yoy_file = pathlib.Path(f"data/series/{series_id}_YOY.csv")
            if yoy_file.exists():
                yoy_df = pd.read_csv(yoy_file)
                yoy_df['date'] = pd.to_datetime(yoy_df['date'])
                yoy_df = yoy_df.set_index('date')
                
                ts_trans = parse_numeric_series(yoy_df['yoy_pct']).dropna()
                if not ts_trans.empty:
                    current_value = float(ts_trans.iloc[-1])
                    last_date = ts_trans.index[-1]
                    print(f"📊 {series_id}: YoY = {current_value:.2f}% (使用预计算数据)")
                    
                    # 基准值和风险评分
                    benchmark_value = calculate_benchmark_corrected(series_id, indicator, ts_trans, crisis_periods)
                    risk_score = calculate_risk_score_simple(current_value, benchmark_value, indicator, ts_trans, scoring_config)
                    
                    return {
                        'name': indicator.get('name', series_id),
                        'series_id': series_id,
                        'group': indicator.get('group', 'unknown'),
                        'current_value': current_value,
                        'benchmark_value': benchmark_value,
                        'risk_score': risk_score,
                        'last_date': str(last_date.date()),
                        'global_weight': indicator.get('weight', 0.0),
                        'higher_is_risk': indicator.get('higher_is_risk', True),
                        'compare_to': indicator.get('compare_to', 'noncrisis_p75'),
                        'plain_explainer': get_indicator_explanation(series_id, indicator)
                    }
        except Exception as e:
            print(f"⚠️ {series_id}: YoY预计算数据读取失败: {e}")

    # 1) 读取原始或合成 ★
    ts = compose_series(series_id)
    if ts is None:
        ts = fetch_series(series_id)
    if ts is None or ts.empty:
        print(f"⚠️ {series_id}: 无数据，跳过处理")
        return None

    # 2) 对历史做同样的 transform ★
    ts_trans = transform_series(series_id, ts, indicator)
    ts_trans = ts_trans.dropna()
    if ts_trans.empty:
        die_if(True, f"{series_id}: 变换后无数据 (transform={indicator.get('transform')})")

    # 3) 取"当前值"= 变换后的最新值（不再依赖本地 YOY CSV）★
    current_value = float(ts_trans.iloc[-1])
    last_date = ts_trans.index[-1]

    # 4) 基准值也在"同域"的 ts_trans 上计算 ★
    benchmark_value = calculate_benchmark_corrected(series_id, indicator, ts_trans, crisis_periods)

    # 5) 风险分：ECDF 在 ts_trans 域内 ★
    risk_score = calculate_risk_score_simple(current_value, benchmark_value, indicator, ts_trans)
    
    # 6) 过期数据降权
    last_dt = ts_trans.index[-1]
    max_lag = indicator.get('max_lag_days')
    if max_lag:
        lag_days = (datetime.now(JST).date() - last_dt.date()).days
        if lag_days > max_lag:
            risk_score *= 0.9
            print(f"⚠️ {series_id}: 数据过期{lag_days}天，风险评分降权至{risk_score:.1f}")

    return {
        'name': indicator.get('name', series_id),
        'series_id': series_id,
        'group': indicator.get('group', 'unknown'),
        'current_value': current_value,
        'benchmark_value': benchmark_value,
        'risk_score': risk_score,
        'last_date': str(last_date.date()),
        'global_weight': indicator.get('weight', 0.0),
        'higher_is_risk': indicator.get('higher_is_risk', True),
        'compare_to': indicator.get('compare_to', 'noncrisis_p75'),
        'plain_explainer': get_indicator_explanation(series_id),
        'role': role
    }

def calculate_benchmark_simple(ts, indicator, crisis_periods):
    """改进的基准值计算，使用危机/非危机掩码，支持可选危机子集"""
    compare_to = indicator.get('compare_to', 'noncrisis_p75')
    
    # 创建危机期掩码
    crisis_mask = pd.Series(False, index=ts.index)
    for crisis in crisis_periods:
        start = pd.Timestamp(crisis["start"])
        end = pd.Timestamp(crisis["end"])
        crisis_mask |= (ts.index >= start) & (ts.index <= end)
    
    # 根据compare_to选择子样本
    if compare_to.startswith('crisis_'):
        sub_ts = ts[crisis_mask]
    elif compare_to.startswith('noncrisis_'):
        sub_ts = ts[~crisis_mask]
    else:
        sub_ts = ts
    
    # 样本不足则回退全样本
    if sub_ts.dropna().size < 24:
        sub_ts = ts
    
    # 计算分位数
    if 'median' in compare_to:
        return float(sub_ts.median())
    elif 'p' in compare_to:
        # 提取百分位数
        p_str = compare_to.split('p')[-1]
        if p_str.isdigit():
            p = float(p_str) / 100.0
            return float(sub_ts.quantile(p))
        else:
            return float(sub_ts.median())
    else:
        return float(sub_ts.median())

def _mask_by_crisis(ts, crisis_periods, use: str):
    """根据危机期间掩码选择子样本"""
    if use not in ['crisis', 'noncrisis', 'all']:
        return ts
    
    # 创建危机期掩码
    crisis_mask = pd.Series(False, index=ts.index)
    for crisis in crisis_periods:
        start = pd.Timestamp(crisis["start"])
        end = pd.Timestamp(crisis["end"])
        crisis_mask |= (ts.index >= start) & (ts.index <= end)
    
    if use == 'crisis':
        return ts[crisis_mask]
    elif use == 'noncrisis':
        return ts[~crisis_mask]
    else:  # 'all'
        return ts

def calculate_benchmark_corrected(series_id, indicator, ts_trans, crisis_periods):
    """
    计算修正后的基准值（使用预计算的YoY基准值）
    """
    try:
        bench_file = pathlib.Path("data/benchmarks_yoy.json")
        if bench_file.exists() and indicator.get("transform")=="yoy_pct":
            benchmarks = json.loads(bench_file.read_text(encoding="utf-8"))
            if series_id in benchmarks:
                return float(benchmarks[series_id])
    except Exception:
        pass
    # 正确回退：把 crisis_periods 传进去，且用 ts_trans ★
    return calculate_benchmark_simple(ts_trans, indicator, crisis_periods)

def _parse_compare_to_to_pct(compare_to: str) -> float:
    """解析 compare_to 为分位数阈值"""
    if compare_to.endswith('_median'):
        return 0.5
    if '_p' in compare_to:
        try:
            return float(compare_to.split('_p')[-1]) / 100.0
        except:
            return 0.5
    return 0.5  # 默认中位数

def score_with_threshold(ts: pd.Series, current: float, *, direction: str, compare_to: str, tail: str='single') -> float:
    """真正用阈值参与打分（单尾/双尾统一）"""
    if ts is None or ts.empty or pd.isna(current):
        return 50.0
    
    # 1) 全样本分位
    p_cur = (ts <= current).mean()
    # 2) 阈值分位
    p_thr = _parse_compare_to_to_pct(compare_to)
    eps = 1e-6
    
    # 3) 映射成 0~100
    if tail == 'both':
        p_mid = 0.5
        denom = max(abs(p_mid - p_thr), eps)
        raw = min(1.0, abs(p_cur - p_mid) / denom)
    else:
        if direction == 'up_is_risk':   # 高为险
            # 当前值越高（p_cur越大），风险越高
            raw = max(0.0, (p_cur - p_thr) / max(1 - p_thr, eps))
        else:                           # 低为险
            # 当前值越低（p_cur越小），风险越高
            # 当 p_cur < p_thr 时，说明当前值低于阈值，风险高
            raw = max(0.0, (p_thr - p_cur) / max(p_thr, eps))
    
    score = float(np.clip(raw * 100.0, 0, 100))
    
    # 修复：对于 down_is_risk，如果当前值低于基准值，确保分数至少反映风险
    if direction == 'down_is_risk' and ts is not None and not ts.empty:
        benchmark_val = ts.quantile(p_thr)
        if current < benchmark_val:
            # 当前值低于基准值，风险应该较高
            # 如果计算出的分数太低，需要调整
            if score < 50:
                # 根据偏离程度计算风险分数
                # 偏离越大，分数越高
                deviation = (benchmark_val - current) / max(abs(benchmark_val), eps)
                # 确保分数至少为 50，并根据偏离程度增加
                score = min(100.0, 50.0 + min(50.0, deviation * 100.0))
    
    return score

def level_from_score(score: float, bands: dict) -> str:
    """根据分数返回风险等级"""
    high = bands.get('high', 80)
    med = bands.get('med', 60)
    low = bands.get('low', 40)
    
    if score >= high:
        return 'high'
    elif score >= med:
        return 'med'
    elif score >= low:
        return 'low'
    else:
        return 'very_low'

def compute_momentum_score(ts: pd.Series, days: list = [1, 5]) -> float:
    """计算动量评分（0-1之间）"""
    if ts is None or ts.empty or len(ts) < max(days) + 1:
        return 0.0
    
    momentum_scores = []
    for d in days:
        if len(ts) > d:
            # 计算d天前的值到当前值的变化
            current = ts.iloc[-1]
            past = ts.iloc[-d-1]
            if not pd.isna(current) and not pd.isna(past) and past != 0:
                change = (current - past) / abs(past)
                momentum_scores.append(abs(change))
    
    if not momentum_scores:
        return 0.0
    
    # 返回平均动量（标准化到0-1）
    avg_momentum = np.mean(momentum_scores)
    return min(1.0, avg_momentum)

# ===== v2.0: 升级的数学引擎 =====

def calculate_momentum_score_v2(ts: pd.Series, momentum_window: int, 
                                 invert_momentum: bool = False, 
                                 higher_is_risk: bool = True) -> float:
    """
    v2.0: 计算动量评分（Rate of Change over momentum_window months）
    
    Args:
        ts: 时间序列数据
        momentum_window: 动量计算窗口（月数）
        invert_momentum: 是否反转动量（快速下降为风险，如流动性指标）
        higher_is_risk: 指标方向（高为险还是低为险）
    
    Returns:
        动量评分 (0-100)
    """
    if ts is None or ts.empty:
        return 0.0
    
    # 转换为月度数据（如果原始数据是日频或周频）
    if len(ts) < momentum_window + 1:
        return 0.0
    
    # 根据数据频率推断期数
    freq = pd.infer_freq(ts.index) or ""
    if freq.startswith('D'):
        periods = momentum_window * 20  # 约20个交易日/月
    elif freq.startswith('W'):
        periods = momentum_window * 4  # 约4周/月
    elif freq.startswith('M'):
        periods = momentum_window
    elif freq.startswith('Q'):
        periods = max(1, momentum_window // 3)
    else:
        periods = momentum_window * 20  # 默认假设日频
    
    if len(ts) < periods + 1:
        return 0.0
    
    # 计算Rate of Change (RoC)
    current = ts.iloc[-1]
    past = ts.iloc[-periods-1]
    
    if pd.isna(current) or pd.isna(past) or past == 0:
        return 0.0
    
    roc = (current - past) / abs(past) * 100  # 转换为百分比
    
    # 根据方向调整
    if invert_momentum:
        # 快速下降为风险（如流动性指标）
        roc = -roc
    
    # 将RoC映射到0-100评分
    # 对于higher_is_risk指标：正RoC增加风险；对于lower_is_risk指标：负RoC增加风险
    if higher_is_risk:
        # 正变化（上升）增加风险
        momentum_score = 50 + min(50, max(-50, roc * 10))  # 放大10倍，限制在0-100
    else:
        # 负变化（下降）增加风险
        momentum_score = 50 + min(50, max(-50, -roc * 10))
    
    return max(0.0, min(100.0, momentum_score))

def calculate_persistence_score(ts: pd.Series, current_score: float, 
                                high_risk_threshold: float = 80.0,
                                persistence_months: int = 3) -> float:
    """
    v2.0: 计算持续时间/持续性评分
    
    如果指标持续处于高风险区域（>threshold）超过persistence_months个月，
    则应用乘数增加风险评分。
    
    Args:
        ts: 时间序列数据
        current_score: 当前风险评分
        high_risk_threshold: 高风险阈值
        persistence_months: 持续高风险月数阈值
    
    Returns:
        调整后的风险评分
    """
    if ts is None or ts.empty or current_score < high_risk_threshold:
        return current_score
    
    # 计算历史评分序列（简化：假设我们只有当前评分）
    # 在实际实现中，需要保存历史评分或从历史数据重新计算
    # 这里使用简化版本：如果当前评分高，检查历史趋势
    
    # 检查最近persistence_months个月的趋势
    freq = pd.infer_freq(ts.index) or ""
    if freq.startswith('D'):
        lookback_periods = persistence_months * 20
    elif freq.startswith('W'):
        lookback_periods = persistence_months * 4
    elif freq.startswith('M'):
        lookback_periods = persistence_months
    else:
        lookback_periods = persistence_months * 20
    
    if len(ts) < lookback_periods:
        return current_score
    
    # 检查最近几个月的值是否持续处于高风险区域
    recent_values = ts.iloc[-lookback_periods:].dropna()
    if len(recent_values) < persistence_months:
        return current_score
    
    # 简化：如果当前值明显高于历史中位数，认为持续高风险
    median_value = recent_values.median()
    current_value = ts.iloc[-1]
    
    if current_value > median_value * 1.1:  # 当前值比中位数高10%以上
        # 应用持续性乘数
        persistence_multiplier = 1.1
        adjusted_score = min(100.0, current_score * persistence_multiplier)
        return adjusted_score
    
    return current_score

# ===== v2.0: 报告层升级 =====

def calculate_trend(ts: pd.Series, current_score: float, previous_score: float = None) -> str:
    """
    v2.0: 计算趋势箭头
    
    Args:
        ts: 时间序列数据
        current_score: 当前风险评分
        previous_score: 上一期风险评分（如果可用）
    
    Returns:
        趋势符号: ↑ (恶化), ↓ (改善), → (稳定)
    """
    if previous_score is not None:
        # 如果有历史评分，直接比较
        if current_score > previous_score + 5:  # 阈值5分
            return "↑"  # 恶化
        elif current_score < previous_score - 5:
            return "↓"  # 改善
        else:
            return "→"  # 稳定
    
    # 如果没有历史评分，使用时间序列数据推断趋势
    if ts is None or ts.empty or len(ts) < 2:
        return "→"
    
    # 计算最近几个数据点的趋势
    recent_values = ts.iloc[-min(5, len(ts)):].dropna()
    if len(recent_values) < 2:
        return "→"
    
    # 简单线性趋势
    if len(recent_values) >= 2:
        first_half = recent_values.iloc[:len(recent_values)//2].mean()
        second_half = recent_values.iloc[len(recent_values)//2:].mean()
        
        if second_half > first_half * 1.02:  # 上升超过2%
            return "↑"
        elif second_half < first_half * 0.98:  # 下降超过2%
            return "↓"
    
    return "→"

def generate_macro_narrative(processed_indicators: list, group_scores: dict, 
                            total_score: float, scoring_config: dict) -> str:
    """
    v2.0: 生成宏观叙事（自动文本）
    
    基于指标状态生成情景分析：
    - Scenario A (滞胀): 通胀 > 阈值 AND 增长 < 阈值
    - Scenario B (流动性陷阱): RRP < 低阈值 AND 利差 > 高阈值
    - Scenario C (分化): 收益率曲线倒挂 BUT 利差收紧
    """
    narratives = []
    
    # 获取关键指标
    inflation_score = 0
    growth_score = 0
    yield_curve_score = 0
    spread_score = 0
    rrp_value = None
    liquidity_score = 0
    
    for indicator in processed_indicators:
        series_id = indicator.get('series_id', '')
        score = indicator.get('risk_score', 0)
        
        # 通胀指标
        if series_id in ['T5YIE', 'CPIAUCSL', 'PCEPI']:
            inflation_score = max(inflation_score, score)
        
        # 增长指标
        if series_id in ['GDP', 'INDPRO', 'PAYEMS', 'NEWORDER']:
            growth_score = max(growth_score, score)
        
        # 收益率曲线
        if series_id in ['T10Y3M', 'T10Y2Y']:
            yield_curve_score = max(yield_curve_score, score)
        
        # 信用利差
        if series_id in ['BAMLH0A0HYM2', 'BAA10YM', 'CP_MINUS_DTB3']:
            spread_score = max(spread_score, score)
        
        # 流动性指标
        if series_id == 'RRPONTSYD':
            rrp_value = indicator.get('current_value')
        if series_id in ['RRPONTSYD', 'WTREGEN', 'M2SL']:
            liquidity_score = max(liquidity_score, score)
    
    # 获取阈值
    bands = scoring_config.get('bands', {'low': 40, 'med': 60, 'high': 80})
    high_threshold = bands.get('high', 80)
    med_threshold = bands.get('med', 60)
    
    # Scenario A: 滞胀压力
    if inflation_score >= med_threshold and growth_score >= med_threshold:
        narratives.append("⚠️ **滞胀压力检测**: 通胀预期上升同时经济增长放缓，可能出现滞胀风险。")
    
    # Scenario B: 流动性陷阱
    if rrp_value is not None and rrp_value < 100:  # RRP接近0
        if spread_score >= med_threshold:
            narratives.append("⚠️ **流动性陷阱警告**: 隔夜逆回购接近零（流动性枯竭）同时信用利差扩大，市场流动性紧张。")
    
    # Scenario C: 市场分化
    if yield_curve_score >= high_threshold and spread_score < med_threshold:
        narratives.append("⚠️ **市场分化**: 收益率曲线倒挂（债券市场预测衰退），但信用利差保持低位（信用市场保持乐观），市场信号出现分化。")
    
    # 共振检测
    yield_curve_groups = ['core_warning', '收益率曲线']
    real_economy_groups = ['real_economy', '实体经济']
    
    yield_curve_high = False
    real_economy_high = False
    
    for group_name, group_data in group_scores.items():
        if any(g in group_name for g in yield_curve_groups):
            if group_data.get('score', 0) >= high_threshold:
                yield_curve_high = True
        if any(g in group_name for g in real_economy_groups):
            if group_data.get('score', 0) >= high_threshold:
                real_economy_high = True
    
    if yield_curve_high and real_economy_high:
        narratives.append("🚨 **系统性风险共振**: 收益率曲线和实体经济同时发出高风险信号，系统性风险上升。")
    
    # 总体风险评估
    if total_score >= high_threshold:
        narratives.append(f"🔴 **总体高风险**: 综合风险评分 {total_score:.1f}/100，建议密切关注市场动态。")
    elif total_score >= med_threshold:
        narratives.append(f"🟡 **中等风险**: 综合风险评分 {total_score:.1f}/100，保持警惕。")
    else:
        narratives.append(f"🟢 **低风险**: 综合风险评分 {total_score:.1f}/100，市场状况相对稳定。")
    
    if not narratives:
        narratives.append("ℹ️ 当前市场信号较为均衡，未检测到明显的系统性风险。")
    
    return "\n\n".join(narratives)

def generate_bubble_diagnosis(processed_indicators: list) -> tuple:
    """
    基于巴菲特指标(长期)、利润背离(中期)和流动性(短期)生成深度泡沫诊断
    
    Args:
        processed_indicators: 处理后的指标列表
    
    Returns:
        (markdown_text, html_text) 元组
    """
    # 1. 提取核心指标数据
    def get_ind_data(sid):
        for ind in processed_indicators:
            if ind.get('series_id') == sid:
                return ind
        return {}
    
    buffett = get_ind_data('BUFFETT_INDICATOR')
    profit_div = get_ind_data('SP500_PROFIT_DIVERGENCE')
    liquidity = get_ind_data('USD_NET_LIQUIDITY')
    
    # 获取数值 (带默认值)
    val_buffett = buffett.get('current_value', 0) if buffett else 0
    score_buffett = buffett.get('risk_score', 0) if buffett else 0
    
    score_profit = profit_div.get('risk_score', 0) if profit_div else 0
    
    val_liq = liquidity.get('current_value', 0) if liquidity else 0
    score_liq = liquidity.get('risk_score', 0) if liquidity else 0
    
    # 2. 诊断逻辑与文案生成（严格区分"状态"与"触发"）
    # 场景 A: 危机临界点 (流动性枯竭 + 估值高) - 触发器被激活
    if score_liq >= 80:
        status = "🔴 危机临界点 (Crash Alert)"
        main_color = "#c62828"
        bg_color = "#ffebee"
        final_judgment = "红色警报！高估值叠加流动性枯竭（触发器被激活）。这是历史上泡沫破裂的标准范式，建议立即防御。"
    # 场景 B: 高估值平稳期 (估值高 + 流动性充裕) - 状态昂贵但无触发
    elif score_profit >= 80:
        status = "🟠 高估值平稳期 (Expensive but Stable)"
        main_color = "#ef6c00"
        bg_color = "#fff3e0"
        final_judgment = "当前处于'高估值、低压力'状态。虽然资产价格昂贵（长期锚红色），但系统流动性充裕（地板绿色），**缺乏触发崩盘的宏观催化剂**。市场可能会在高位维持较长时间，而非立即崩盘。"
    # 场景 C: 安全/正常
    else:
        status = "🟢 趋势正常"
        main_color = "#2e7d32"
        bg_color = "#e8f5e9"
        final_judgment = "估值虽高，但盈利与流动性配合良好，暂无系统性崩盘迹象。"
    
    # 3. 生成 HTML 卡片 (三段式结构)
    html_card = f"""
    <div style="background-color: {bg_color}; border-left: 5px solid {main_color}; padding: 15px; margin: 20px 0; border-radius: 4px;">
        <h3 style="color: {main_color}; margin-top: 0;">💹 美股泡沫深度诊断 (US Bubble Diagnosis)</h3>
        <p><strong>当前状态：{status}</strong></p>
        <hr style="border-top: 1px dashed {main_color}; opacity: 0.3;">
        
        <p><strong>逻辑一：长期估值锚 (The Ceiling)</strong><br>
        <span style="color: #666; font-size: 0.9em;">原理：巴菲特指标(市值/GDP)衡量股市长期是否"昂贵"。</span><br>
        <strong>现状</strong>：系统评分 <b>{score_buffett:.1f}</b> (🔴 历史极高位)。当前比值 <b>{val_buffett:.1f}%</b>。<br>
        👉 <em>结论：长期估值已达历史天花板，容错率极低。</em></p>

        <p><strong>逻辑二：盈利背离度 (The Foam)</strong><br>
        <span style="color: #666; font-size: 0.9em;">原理：股价涨幅远超利润增速("Optimism"阶段)是泡沫化的核心特征。</span><br>
        <strong>现状</strong>：系统评分 <b>{score_profit:.1f}</b> (🔴 高风险)。<br>
        👉 <em>结论：股价上涨脱离基本面（P/E扩张驱动），存在明显的投机溢价。</em></p>

        <p><strong>逻辑三：流动性底座 (The Floor)</strong><br>
        <span style="color: #666; font-size: 0.9em;">原理：只要央行/财政部还在注入流动性，泡沫就可以维持("Rational Bubble")。</span><br>
        <strong>现状</strong>：系统评分 <b>{score_liq:.1f}</b> ({"🔵 极低风险" if score_liq < 40 else "🟡 中风险"})。净流动性同比 <b>{val_liq:.2f}%</b>。<br>
        👉 <em>结论：流动性尚未枯竭，仍在为高估值提供资金接力。</em></p>
        
        <hr style="border-top: 1px dashed {main_color}; opacity: 0.3;">
        <p style="font-weight: bold; color: {main_color};">🤖 系统最终预判：<br>
        "{final_judgment}"</p>
        <p style="font-size: 0.85em; color: #666; margin-top: 10px; font-style: italic;">
        <em>注：本模块仅评估'是否具备危机条件'，不代表对短期涨跌的预测。估值高低决定下跌空间，流动性松紧决定下跌时间。</em></p>
    </div>
    """
    
    # 生成 Markdown 文本
    md_text = f"""### 💹 美股泡沫深度诊断

**当前状态：{status}**

**逻辑一：长期估值锚 (The Ceiling)**
* **现状**：巴菲特指标 **{val_buffett:.1f}%** (评分 {score_buffett:.1f})。
* **结论**：长期估值已达历史天花板。

**逻辑二：盈利背离度 (The Foam)**
* **现状**：利润背离评分 **{score_profit:.1f}** (🔴 高风险)。
* **结论**：股价脱离基本面，全靠拔估值。

**逻辑三：流动性底座 (The Floor)**
* **现状**：流动性评分 **{score_liq:.1f}**。
* **结论**：流动性尚未枯竭，泡沫暂未破裂。

> **🤖 系统预判**：{final_judgment}

*注：本模块仅评估'是否具备危机条件'，不代表对短期涨跌的预测。估值高低决定下跌空间，流动性松紧决定下跌时间。*

---
"""
    return md_text, html_card

def generate_market_regime_diagnosis(processed_indicators: list) -> tuple:
    """
    v3.0: 生成市场体制雷达诊断
    
    Args:
        processed_indicators: 处理后的指标列表（可能不包含市场指标）
    
    Returns:
        (markdown_text, html_text) 元组
    """
    # 直接从文件读取市场体制指标数据（不依赖processed_indicators）
    def load_market_indicator(sid):
        """直接从CSV文件加载市场指标的最新值"""
        try:
            csv_path = BASE / "data" / "series" / f"{sid}.csv"
            if csv_path.exists():
                # 尝试两种格式：带date列（新格式）或索引为日期（旧格式）
                df = pd.read_csv(csv_path)
                if not df.empty:
                    # 新格式：date, value 两列
                    if 'date' in df.columns and 'value' in df.columns:
                        latest_value = float(df['value'].iloc[-1])
                    # 旧格式：索引为日期，第一列为value
                    elif 'value' in df.columns:
                        latest_value = float(df['value'].iloc[-1])
                    elif len(df.columns) == 1:
                        latest_value = float(df.iloc[-1, 0])
                    else:
                        latest_value = 0.0
                    print(f"📊 市场指标 {sid}: {latest_value}")
                    return latest_value
            else:
                print(f"⚠️ 市场指标文件不存在: {csv_path}")
        except Exception as e:
            print(f"⚠️ 加载市场指标 {sid} 失败: {e}")
        return 0.0
    
    # 获取市场体制指标数据
    val_trend = load_market_indicator('MKT_SPY_TREND_STATUS')
    val_vol = load_market_indicator('MKT_SPY_REALIZED_VOL')
    val_credit = load_market_indicator('MKT_CREDIT_APPETITE')
    
    # 判断趋势状态
    trend_status = "🟢 牛市 (价格 > 200日均线)" if val_trend >= 0.5 else "🔴 熊市 (价格 < 200日均线)"
    trend_color = "#2e7d32" if val_trend >= 0.5 else "#c62828"
    
    # 判断波动率环境（假设 >20% 为高波动）
    vol_status = "🔴 高波动 (波动率飙升)" if val_vol > 20 else "🟢 平静 (波动率正常)" if val_vol < 15 else "🟡 中等波动"
    vol_color = "#c62828" if val_vol > 20 else "#2e7d32" if val_vol < 15 else "#f57c00"
    
    # 判断风险偏好（HYG/TLT 比率，通常范围在 0.8-1.2 之间）
    # 需要与历史均值对比，这里简化处理：>0.9 为风险偏好，<0.9 为避险
    credit_status = "🟢 风险偏好 (资金进攻)" if val_credit > 0.9 else "🔴 避险主导 (资金撤退)" if val_credit > 0 else "🟡 中性"
    credit_color = "#2e7d32" if val_credit > 0.9 else "#c62828" if val_credit > 0 else "#f57c00"
    
    # AI 综评逻辑
    if val_trend < 0.5 and val_vol > 20:
        ai_judgment = "当前处于'熊市高波动'的市场体制，宏观数据虽未恶化，但价格行为显示资金正在快速撤退，市场情绪极度恐慌。"
        main_color = "#c62828"
        bg_color = "#ffebee"
    elif val_trend < 0.5 and val_credit < 0:
        ai_judgment = "当前处于'宽幅震荡/避险主导'的市场体制，宏观数据虽未恶化，但价格行为显示资金正在撤退，风险资产承压。"
        main_color = "#ef6c00"
        bg_color = "#fff3e0"
    elif val_trend >= 0.5 and val_vol < 15:
        ai_judgment = "当前处于'牛市平静'的市场体制，价格行为与宏观数据一致，市场情绪稳定，资金持续流入风险资产。"
        main_color = "#2e7d32"
        bg_color = "#e8f5e9"
    else:
        ai_judgment = "当前处于'震荡分化'的市场体制，价格行为与宏观数据出现分化，需要密切关注后续信号确认方向。"
        main_color = "#f57c00"
        bg_color = "#fff3e0"
    
    # 生成 HTML 卡片
    html_card = f"""
    <div style="background-color: {bg_color}; border-left: 5px solid {main_color}; padding: 15px; margin: 20px 0; border-radius: 4px;">
        <h3 style="color: {main_color}; margin-top: 0;">🧭 市场体制雷达 (Market Regime Radar)</h3>
        
        <p><strong>主要趋势 (Trend)</strong><br>
        <span style="color: #666; font-size: 0.9em;">原理：SPY 价格相对于 200日均线的位置反映长期趋势方向。</span><br>
        <strong>现状</strong>：{trend_status} (当前值: {val_trend:.0f})<br>
        👉 <em>结论：长期趋势方向已明确。</em></p>

        <p><strong>波动率环境 (Volatility)</strong><br>
        <span style="color: #666; font-size: 0.9em;">原理：20日实际波动率反映市场恐慌程度和价格波动幅度。</span><br>
        <strong>现状</strong>：{vol_status} (当前值: {val_vol:.2f}%)<br>
        👉 <em>结论：波动率环境决定了交易策略的激进程度。</em></p>

        <p><strong>风险偏好 (Risk Appetite)</strong><br>
        <span style="color: #666; font-size: 0.9em;">原理：HYG/TLT 比率反映资金在风险资产与避险资产之间的配置偏好。</span><br>
        <strong>现状</strong>：{credit_status} (当前值: {val_credit:.4f})<br>
        👉 <em>结论：资金流向揭示了市场的真实情绪。</em></p>
        
        <hr style="border-top: 1px dashed {main_color}; opacity: 0.3;">
        <p style="font-weight: bold; color: {main_color};">🤖 AI 综评：<br>
        "{ai_judgment}"</p>
    </div>
    """
    
    # 生成 Markdown 文本
    md_text = f"""### 🧭 市场体制雷达 (Market Regime Radar)

**主要趋势 (Trend)**
* **现状**：{trend_status} (当前值: {val_trend:.0f})
* **结论**：长期趋势方向已明确。

**波动率环境 (Volatility)**
* **现状**：{vol_status} (当前值: {val_vol:.2f}%)
* **结论**：波动率环境决定了交易策略的激进程度。

**风险偏好 (Risk Appetite)**
* **现状**：{credit_status} (当前值: {val_credit:.4f})
* **结论**：资金流向揭示了市场的真实情绪。

> **🤖 AI 综评**：{ai_judgment}

---
"""
    return md_text, html_card

def generate_gold_diagnosis(processed_indicators: list) -> tuple:
    """
    生成黄金市场深度诊断（三段式逻辑）
    
    Args:
        processed_indicators: 处理后的指标列表
    
    Returns:
        (html_text, markdown_text) 元组
    """
    def get_data(sid):
        for i in processed_indicators:
            if i.get('series_id') == sid:
                return i
        return {}
    
    # 获取数据
    gold_val = get_data('GOLD_REAL_RATE_DIFF')
    real_rate = get_data('US_REAL_RATE_10Y')
    fiscal = get_data('MTSDS133FMS')
    
    # 提取数值 (带默认值防止报错)
    val_diff = gold_val.get('current_value', 0) if gold_val else 0
    score_diff = gold_val.get('risk_score', 0) if gold_val else 0
    
    val_rate = real_rate.get('current_value', 0) if real_rate else 0
    score_rate = real_rate.get('risk_score', 0) if real_rate else 0
    
    val_fiscal = fiscal.get('current_value', 0) if fiscal else 0
    
    # 定义状态颜色
    status_color = "#d32f2f" if score_diff > 80 else "#f57c00"  # 红或橙
    
    # 生成HTML文本
    html = f"""
    <div style="background-color: #fff8e1; border-left: 5px solid #ffc107; padding: 15px; margin: 20px 0; border-radius: 4px;">
        <h3 style="color: #bf360c; margin-top: 0;">🏆 黄金见顶逻辑诊断 (Gold Top Diagnosis)</h3>
        
        <p><strong>逻辑一：估值泡沫 (泡沫的燃料)</strong><br>
        <span style="color: #666; font-size: 0.9em;">原理：当金价远超"赤字+购金"模型时，产生的残差代表纯粹的情绪溢价。</span><br>
        <strong>现状</strong>：系统评分 <b>{score_diff:.1f}</b> (🔴 高风险)。当前残差 <b>{val_diff:.2f}</b> 远超历史警戒线。<br>
        👉 <em>结论：金价处于极度泡沫化阶段，脱离了基本面模型。</em></p>

        <p><strong>逻辑二：实际利率引信 (泡沫的针)</strong><br>
        <span style="color: #666; font-size: 0.9em;">原理：实际利率是持有黄金的机会成本，大幅上行往往刺破泡沫。</span><br>
        <strong>现状</strong>：系统评分 <b>{score_rate:.1f}</b> ({"🔵 极低风险" if score_rate < 40 else "🟡 中风险"})。当前实际利率 <b>{val_rate:.2f}%</b>。<br>
        👉 <em>结论：尚未满足顶部特征。历史上的黄金大顶通常需要实际利率突破 2.0%~2.5% 且财政显著收缩。当前仅满足估值条件，未满足宏观约束条件。</em></p>

        <p><strong>逻辑三：财政脉冲熄火 (领先指标)</strong><br>
        <span style="color: #666; font-size: 0.9em;">原理：财政赤字收缩往往领先金价见顶 6-9 个月。</span><br>
        <strong>现状</strong>：当前赤字 <b>{val_fiscal/1000:.2f} Billion</b>。<br>
        👉 <em>结论：财政尚未显著收缩，仍在为市场提供流动性支撑。</em></p>
        
        <hr style="border-top: 1px dashed #bdbdbd;">
        <p style="font-weight: bold; color: #bf360c;">🤖 系统最终预判：<br>
        "黄金处于<strong>估值极端区</strong>，但在宏观条件（利率/财政）配合之前，这种背离可能持续。<strong>当前是'且涨且珍惜'的鱼尾阶段，而非确定的反转时刻。</strong>"</p>
    </div>
    """
    
    # 生成Markdown文本
    md = f"""### 🏆 黄金见顶逻辑诊断

**逻辑一：估值泡沫 (泡沫的燃料)**
* **现状**：评分 **{score_diff:.1f}** (🔴 高风险)，残差 **{val_diff:.2f}**。
* **结论**：金价处于极度泡沫化阶段，脱离基本面。

**逻辑二：实际利率引信 (泡沫的针)**
* **现状**：评分 **{score_rate:.1f}**，当前值 **{val_rate:.2f}%**。
* **结论**：尚未满足顶部特征。历史上的黄金大顶通常需要实际利率突破 2.0%~2.5% 且财政显著收缩。当前仅满足估值条件，未满足宏观约束条件。

**逻辑三：财政脉冲**
* **现状**：赤字 **{val_fiscal/1000:.2f} B**，尚未显著收缩。

> **🤖 系统预判**：黄金处于**估值极端区**，但在宏观条件（利率/财政）配合之前，这种背离可能持续。**当前是'且涨且珍惜'的鱼尾阶段，而非确定的反转时刻。**
"""
    
    return html, md

def calculate_resonance(group_scores: dict, processed_indicators: list,
                        resonance_threshold: float = 80.0,
                        systemic_risk_multiplier: float = 1.2) -> float:
    """
    v2.0: 计算共振检测
    
    检查"收益率曲线"组和"实体经济"组是否同时发出高风险信号（>threshold）。
    如果True，应用系统性风险乘数。
    
    Args:
        group_scores: 分组评分字典
        processed_indicators: 处理后的指标列表
        resonance_threshold: 触发共振的高风险阈值
        systemic_risk_multiplier: 系统性风险乘数
    
    Returns:
        调整后的总评分乘数（1.0或systemic_risk_multiplier）
    """
    # 检查收益率曲线组
    yield_curve_groups = ['core_warning', '收益率曲线']  # 可能的组名
    real_economy_groups = ['real_economy', '实体经济']  # 可能的组名
    
    yield_curve_high_risk = False
    real_economy_high_risk = False
    
    # 检查分组评分
    for group_name, group_data in group_scores.items():
        if any(g in group_name for g in yield_curve_groups):
            if group_data.get('score', 0) >= resonance_threshold:
                yield_curve_high_risk = True
        if any(g in group_name for g in real_economy_groups):
            if group_data.get('score', 0) >= resonance_threshold:
                real_economy_high_risk = True
    
    # 如果两个组都高风险，应用共振乘数
    if yield_curve_high_risk and real_economy_high_risk:
        return systemic_risk_multiplier
    
    return 1.0

def calculate_risk_score_simple(current, benchmark, indicator, ts=None, scoring_config=None):
    """
    v2.0: 升级的风险评分计算
    从 Score = f(Level) 升级到 Score = f(Level, Momentum, Duration)
    """
    compare_to = indicator.get('compare_to', 'noncrisis_median')
    direction = 'up_is_risk' if indicator.get('higher_is_risk', True) else 'down_is_risk'
    tail = indicator.get('tail', 'single')
    
    # 1. 计算基础水平评分
    if ts is not None and not ts.empty:
        level_score = score_with_threshold(ts, current, direction=direction, compare_to=compare_to, tail=tail)
    else:
        # 回退到简化计算（当没有时间序列数据时）
        higher_is_risk = indicator.get('higher_is_risk', True)
        if higher_is_risk:
            deviation = current - benchmark
        else:
            deviation = benchmark - current
        level_score = 50 + 10 * deviation
        level_score = max(0, min(100, level_score))
    
    # 2. v2.0: 计算动量评分
    momentum_score = 0.0
    if scoring_config and ts is not None and not ts.empty:
        # 获取v2.0配置
        momentum_window = indicator.get('momentum_window', 3)  # 默认3个月
        invert_momentum = indicator.get('invert_momentum', False)
        higher_is_risk = indicator.get('higher_is_risk', True)
        
        # 使用v2.0动量计算
        momentum_score = calculate_momentum_score_v2(
            ts, momentum_window, invert_momentum, higher_is_risk
        )
        
        # 获取权重配置
        momentum_weight = scoring_config.get('momentum_weight', 0.3)
        level_weight = scoring_config.get('level_weight', 0.7)
        
        # 组合评分：Final_Score = level_weight * Level_Score + momentum_weight * Momentum_Score
        final_score = level_weight * level_score + momentum_weight * momentum_score
    else:
        # 如果没有配置或数据，使用旧逻辑（向后兼容）
        if scoring_config and ts is not None and not ts.empty:
            mom_bonus_max = scoring_config.get('momentum_bonus_max', 5)
            momentum_days = scoring_config.get('momentum_days', [1, 5])
            
            momentum_raw = compute_momentum_score(ts, days=momentum_days)
            momentum_bonus = momentum_raw * mom_bonus_max
            
            final_score = min(100.0, level_score + momentum_bonus)
        else:
            final_score = level_score
    
    # 3. v2.0: 应用持续时间逻辑
    if scoring_config and ts is not None and not ts.empty:
        persistence_months = scoring_config.get('persistence_months', 3)
        high_risk_threshold = scoring_config.get('bands', {}).get('high', 80)
        
        final_score = calculate_persistence_score(
            ts, final_score, high_risk_threshold, persistence_months
        )
    
    return max(0.0, min(100.0, final_score))

def calculate_crisis_stats(ts, crisis_periods):
    """计算危机期间的统计数据"""
    if ts.empty:
        return {}
    
    # 创建危机期掩码
    crisis_mask = pd.Series(False, index=ts.index)
    for crisis in crisis_periods:
        start = pd.Timestamp(crisis["start"])
        end = pd.Timestamp(crisis["end"])
        crisis_mask |= (ts.index >= start) & (ts.index <= end)
    
    crisis_vals = ts[crisis_mask].dropna()
    
    if crisis_vals.empty:
        return {}
    
    return {
        "crisis_median": float(np.nanmedian(crisis_vals.values)),
        "crisis_p25": float(np.nanpercentile(crisis_vals.values, 25)),
        "crisis_p75": float(np.nanpercentile(crisis_vals.values, 75)),
        "crisis_mean": float(np.nanmean(crisis_vals.values)),
        "crisis_std": float(np.nanstd(crisis_vals.values, ddof=1)),
        "crisis_n": int(crisis_vals.size)
    }

def get_indicator_explanation(series_id, indicator_config=None):
    """获取指标解释（统一与方向/双尾配置）"""
    explanations = {
        'T10Y3M': '10年期与3个月国债收益率利差。计算公式：T10Y3M = 10年期国债收益率 - 3个月国债收益率。**警惕：** 该指标不仅在"深度倒挂"时预警，在**"倒挂回正（解除倒挂）"**的初期往往对应衰退实质性发生的时刻（Bear Steepening）。当前高风险评分反映了曲线形态的极端不稳定性，而非单纯的倒挂深度。',
        'T10Y2Y': '10年期与2年期国债收益率利差。计算公式：T10Y2Y = 10年期国债收益率 - 2年期国债收益率。倒挂(负值)越深越危险，是重要的经济领先指标。',
        'FEDFUNDS': '联邦基金利率。水平值，利率越高表示货币政策越紧缩，会抑制经济增长。',
        'DTB3': '3个月国债收益率。水平值，收益率越高表示短期利率水平越高。',
        'DGS10': '10年期国债收益率。水平值，收益率越高表示长期利率水平越高。',
        'MORTGAGE30US': '30年期抵押贷款利率。水平值，利率越高表示住房融资成本越高。',
        'SOFR': '有担保隔夜融资利率。水平值，利率越高表示短期融资成本越高。',
        'BAMLH0A0HYM2': '高收益债券风险溢价。计算公式：BAMLH0A0HYM2 = 高收益债券收益率 - 10年期国债收益率。溢价越高表明信用风险上升。',
        'BAA10YM': 'Baa级公司债券与10年期国债利差。计算公式：BAA10YM = Baa级公司债券收益率 - 10年期国债收益率。利差越大表明信用风险上升。',
        'NFCI': '芝加哥金融状况指数。综合多个金融市场指标的合成指数，越高表示金融条件越紧张。',
        'VIXCLS': 'VIX波动率指数。水平值，指数越高表示市场恐慌情绪越严重。',
        'PAYEMS': '非农就业人数同比变化率。计算公式：PAYEMS_YoY = (当前月就业人数 - 上年同期就业人数) / 上年同期就业人数 × 100%。增速越低表示劳动力市场越疲弱。',
        'INDPRO': '工业生产指数同比变化率。计算公式：INDPRO_YoY = (当前月工业生产指数 - 上年同期工业生产指数) / 上年同期工业生产指数 × 100%。增速越低表示工业生产越疲弱。',
        'GDP': '国内生产总值同比变化率。计算公式：GDP_YoY = (当前季度GDP - 上年同期GDP) / 上年同期GDP × 100%。增速越低表示经济增长越疲弱。',
        'HOUST': '新屋开工数。水平值，开工数越低表示房地产投资越疲弱。',
        'CSUSHPINSA': 'Case-Shiller房价指数同比变化率。计算公式：CSUSHPINSA_YoY = (当前月房价指数 - 上年同期房价指数) / 上年同期房价指数 × 100%。双尾风险：暴涨(泡沫)和骤跌(去杠杆)都危险。',
        'UMCSENT': '密歇根消费者信心指数。水平值，指数越低表示消费者情绪越悲观。',
        'TOTALSA': '消费者信贷同比变化率。计算公式：TOTALSA_YoY = (当前月消费者信贷 - 上年同期消费者信贷) / 上年同期消费者信贷 × 100%。双尾风险：暴涨(积累脆弱性)和骤冷(信贷闸门收紧)都危险。',
        'TOTLL': '银行总贷款和租赁同比变化率。计算公式：TOTLL_YoY = (当前周贷款总额 - 上年同期贷款总额) / 上年同期贷款总额 × 100%。双尾风险：暴涨(积累脆弱性)和骤冷(信贷闸门收紧)都危险。',
        'WALCL': '美联储总资产同比变化率。计算公式：WALCL_YoY = (当前周总资产 - 上年同期总资产) / 上年同期总资产 × 100%。增速越高表示扩表速度越快。',
        'RRPONTSYD': '隔夜逆回购协议余额。它是市场的"剩余流动性蓄水池"。当前评分较高是因为余额处于低位，意味着缓冲垫变薄。**注意：RRP下降本身属于资金释放（利好），只有当其与"净流动性"同步收缩时，才构成系统性压力。**',
        'DTWEXBGS': '贸易加权美元指数同比变化率。计算公式：DTWEXBGS_YoY = (当前周美元指数 - 上年同期美元指数) / 上年同期美元指数 × 100%。增速越高表示美元越强势，金融条件越紧张。',
        'CORPDEBT_GDP_PCT': '企业债务占GDP比例。计算公式：CORPDEBT_GDP_PCT = 企业债务总额 / GDP × 100%。比例过高表明企业杠杆率过高。',
        'RESERVES_DEPOSITS_PCT': '银行准备金占存款比例。计算公式：RESERVES_DEPOSITS_PCT = 银行准备金 / 银行存款 × 100%。比例过低表明银行流动性不足。',
        'RESERVES_ASSETS_PCT': '银行准备金占总资产比例。计算公式：RESERVES_ASSETS_PCT = 银行准备金 / 银行总资产 × 100%。比例过低表明银行流动性不足。',
        'TDSP': '家庭债务偿付收入比。水平值，比率越高表示家庭债务负担过重。',
        'DRSFRMACBS': '房贷违约率。水平值，违约率越高表示家庭财务压力越大。',
        # 黄金见顶判断模型指标
        'US_REAL_RATE_10Y': '美国10年期实际利率。计算公式：US_REAL_RATE_10Y = DGS10（名义利率） - T10YIE（通胀预期）。它是持有黄金的机会成本。实际利率飙升意味着持有无息资产（黄金）的成本极高，通常对应金价见顶。实际利率为负或极低时，利好黄金。典型范围：-1.0% - 2.5%，危机阈值：> 2.0%（高位预警）。',
        'MTSDS133FMS': '联邦财政盈余/赤字。美国联邦政府的月度财政收支状况。如果赤字快速收窄（数值趋向0或变正），意味着"财政脉冲"消退，经济失去支撑，且通胀预期下降，利空金价。赤字大幅扩张（数值很负）通常对应"大放水"，利好黄金。典型范围：-300 Billion - 0，危机阈值：> -50 Billion（赤字显著收窄）。',
        'GOLD_REAL_RATE_DIFF': '金价-实际利率估值差。黄金价格与实际利率的相对位置模型残差。当实际利率走高（利空）但金价依然狂涨时，二者差值拉大，形成"鳄鱼嘴"背离，暗示金价进入纯情绪博弈的泡沫阶段，见顶风险极大。差值收敛意味着估值回归合理。典型范围：需根据历史数据动态评估，危机阈值：> 历史90分位。',
        # 权益周期监控指标
        'SP500_PROFIT_DIVERGENCE': '标普500与企业利润背离度。计算公式：SP500_PROFIT_DIVERGENCE = SP500 / CPATAX（企业税后利润）。该指标专门识别"Optimism"阶段（Real Price Return > Real EPS Growth），即股价涨幅远超盈利增长，形成估值泡沫。比值越高，说明股价脱离盈利基本面，见顶风险越大。典型范围：需根据历史数据动态评估，危机阈值：> 历史90分位。',
        'USD_NET_LIQUIDITY': '美联储净流动性。计算公式：USD_NET_LIQUIDITY = WALCL（美联储总资产） - WTREGEN（财政部一般账户） - RRPONTSYD（隔夜逆回购）。该指标反映真正流入市场的流动性。流动性收缩（负增长或快速下跌）是股市最大的风险，意味着市场失去资金支撑。流动性扩张时利好股市。典型范围：需根据历史数据动态评估，危机阈值：YoY < 历史25分位（流动性水位过低）。',
        'STLFSI3': '圣路易斯金融压力指数。该指数综合反映金融市场的压力水平，>0 代表压力高于历史平均。压力飙升通常对应市场恐慌、流动性枯竭，是股市见顶的重要信号。典型范围：-3.0 - 3.0，危机阈值：> 历史90分位（压力飙升）。',
        # 长期估值锚指标
        'BUFFETT_INDICATOR': '巴菲特指标（市值/GDP）。计算公式：BUFFETT_INDICATOR = (WILL5000INDFC / GDP) × 100。该指标反映股票市场总市值相对于经济总量的估值水平，是长期估值锚点。由于全球化、无形资产等因素，该指标可能长期处于高位，但仍可作为结构性估值参考。典型范围：80% - 150%，危机阈值：> 历史90分位（长期估值昂贵）。'
    }
    
    base_explanation = explanations.get(series_id, f'{series_id}指标')
    
    # 根据配置添加方向性说明
    if indicator_config:
        higher_is_risk = indicator_config.get('higher_is_risk', True)
        tail = indicator_config.get('tail', 'single')
        
        if tail == 'both':
            dir_txt = " | 双尾风险"
        elif higher_is_risk:
            dir_txt = " | 高为险"
        else:
            dir_txt = " | 低为险"
        
        return base_explanation + dir_txt
    
    return base_explanation

def run_data_pipeline():
    """运行完整的数据管道：下载+预处理+计算"""
    import subprocess
    import time
    import os

    print("🔄 启动数据管道...")
    print("=" * 60)
    
    # 修复：清洗预计算的中间文件缓存（排除市场信号文件）
    print("🧹 清洗预计算缓存文件...")
    cache_dir = BASE / "data" / "series"
    if cache_dir.exists():
        cache_files = list(cache_dir.glob("*.csv"))
        # 排除 README、市场信号文件（MKT_开头）和其他非数据文件
        cache_files = [f for f in cache_files 
                      if f.name not in ['README.md', 'data_catalog.py'] 
                      and not f.name.startswith('MKT_')]  # v3.0: 保护市场信号文件
        
        removed_count = 0
        for cache_file in cache_files:
            try:
                cache_file.unlink()
                removed_count += 1
            except Exception as e:
                print(f"⚠️ 删除缓存文件失败 {cache_file.name}: {e}")
        
        if removed_count > 0:
            print(f"✅ 已删除 {removed_count} 个预计算缓存文件")
        else:
            print("ℹ️ 没有需要清理的缓存文件")
    else:
        print("ℹ️ 缓存目录不存在，跳过清理")
    
    # 保留原始数据：data/fred/series/*/raw.csv 不会被删除

    total_steps = 4
    current_step = 0

    # 获取当前工作目录
    current_dir = os.getcwd()
    print(f"📁 当前工作目录: {current_dir}")

    # 1. 运行FRED数据下载
    current_step += 1
    progress = int((current_step / total_steps) * 100)
    print(f"📥 步骤{current_step}/{total_steps} ({progress}%): 下载FRED数据...")
    print("⏳ 预计等待时间: 2-5分钟...")

    try:
        script_path = os.path.join(current_dir, "scripts", "sync_fred_http.py")
        print(f"🔍 执行脚本: {script_path}")
        result = subprocess.run([sys.executable, script_path],
                              capture_output=True, text=True, timeout=300, cwd=current_dir)
        if result.returncode == 0:
            print("✅ FRED数据下载完成")
        else:
            print(f"⚠️ FRED数据下载警告: {result.stderr}")
    except Exception as e:
        print(f"❌ FRED数据下载失败: {e}")

    # 2. 运行企业债/GDP比率计算
    current_step += 1
    progress = int((current_step / total_steps) * 100)
    print(f"🧮 步骤{current_step}/{total_steps} ({progress}%): 计算企业债/GDP比率...")
    print("⏳ 预计等待时间: 30秒...")

    try:
        script_path = os.path.join(current_dir, "scripts", "calculate_corporate_debt_gdp_ratio.py")
        print(f"🔍 执行脚本: {script_path}")
        result = subprocess.run([sys.executable, script_path],
                              capture_output=True, text=True, timeout=60, cwd=current_dir)
        if result.returncode == 0:
            print("✅ 企业债/GDP比率计算完成")
        else:
            print(f"⚠️ 企业债/GDP比率计算警告: {result.stderr}")
    except Exception as e:
        print(f"❌ 企业债/GDP比率计算失败: {e}")

    # 3. 运行YoY指标计算
    current_step += 1
    progress = int((current_step / total_steps) * 100)
    print(f"📊 步骤{current_step}/{total_steps} ({progress}%): 计算YoY指标...")
    print("⏳ 预计等待时间: 1-2分钟...")

    try:
        script_path = os.path.join(current_dir, "scripts", "calculate_yoy_indicators.py")
        print(f"🔍 执行脚本: {script_path}")
        result = subprocess.run([sys.executable, script_path],
                              capture_output=True, text=True, timeout=120, cwd=current_dir)
        if result.returncode == 0:
            print("✅ YoY指标计算完成")
        else:
            print(f"⚠️ YoY指标计算警告: {result.stderr}")
    except Exception as e:
        print(f"❌ YoY指标计算失败: {e}")

    # 4. 数据管道完成
    current_step += 1
    progress = int((current_step / total_steps) * 100)
    print(f"✅ 步骤{current_step}/{total_steps} ({progress}%): 数据管道完成")
    print("=" * 60)

def generate_detailed_explanation_report(processed_indicators, output_path, timestamp):
    """生成详细解释报告，包含每个指标的通俗解释"""
    
    # 指标解释字典
    indicator_explanations = {
        'T10Y3M': {
            'name': '10年期-3个月国债利差',
            'description': '长期国债收益率减去短期国债收益率，反映市场对未来经济的预期',
            'high_risk_explanation': '利差很小或为负（倒挂）通常预示经济衰退，因为市场预期未来利率会下降',
            'low_risk_explanation': '利差较大表示经济健康，银行可以正常放贷获利',
            'unit': '%',
            'typical_range': '0.5% - 3.0%',
            'crisis_threshold': '< 0.5%'
        },
        'T10Y2Y': {
            'name': '10年期-2年期国债利差',
            'description': '另一个重要的收益率曲线指标，同样反映经济预期',
            'high_risk_explanation': '倒挂（负值）预示经济衰退风险',
            'low_risk_explanation': '正常利差表示经济健康',
            'unit': '%',
            'typical_range': '0.3% - 2.5%',
            'crisis_threshold': '< 0.3%'
        },
        'BAMLH0A0HYM2': {
            'name': '高收益债券利差',
            'description': '高风险公司债券与国债的收益率差异，反映信用风险',
            'high_risk_explanation': '利差扩大表示企业违约风险上升，投资者要求更高回报',
            'low_risk_explanation': '利差较小表示企业信用状况良好',
            'unit': '%',
            'typical_range': '3% - 8%',
            'crisis_threshold': '> 8%'
        },
        'BAA10YM': {
            'name': '投资级公司债利差',
            'description': '投资级公司债券与国债的收益率差异',
            'high_risk_explanation': '利差扩大表示企业信用风险上升',
            'low_risk_explanation': '利差较小表示企业信用状况良好',
            'unit': '%',
            'typical_range': '1% - 4%',
            'crisis_threshold': '> 4%'
        },
        'TEDRATE': {
            'name': 'TED利差',
            'description': '3个月期银行间拆借利率与国债利率的差异，反映银行间信用风险',
            'high_risk_explanation': '利差扩大表示银行间不信任，流动性紧张',
            'low_risk_explanation': '利差较小表示银行间信任度高，流动性充足',
            'unit': '%',
            'typical_range': '0.1% - 1.0%',
            'crisis_threshold': '> 1.0%'
        },
        'VIXCLS': {
            'name': 'VIX波动率指数',
            'description': '衡量市场恐慌情绪和未来波动率预期',
            'high_risk_explanation': 'VIX高表示市场恐慌，投资者预期波动率上升',
            'low_risk_explanation': 'VIX低表示市场平静，投资者情绪稳定',
            'unit': '点',
            'typical_range': '10 - 30',
            'crisis_threshold': '> 30'
        },
        'NFCI': {
            'name': '芝加哥金融状况指数',
            'description': '综合反映金融市场压力状况',
            'high_risk_explanation': '指数高表示金融状况紧张，融资困难',
            'low_risk_explanation': '指数低表示金融状况宽松，融资容易',
            'unit': '指数',
            'typical_range': '-1.0 - 1.0',
            'crisis_threshold': '> 1.0'
        },
        'GDP': {
            'name': '国内生产总值',
            'description': '衡量一个国家经济总量的指标',
            'high_risk_explanation': 'GDP增长缓慢或负增长表示经济衰退',
            'low_risk_explanation': 'GDP稳定增长表示经济健康',
            'unit': '%',
            'typical_range': '2% - 4%',
            'crisis_threshold': '< 1%'
        },
        'INDPRO': {
            'name': '工业生产指数',
            'description': '衡量制造业和采矿业产出',
            'high_risk_explanation': '工业生产下降表示经济衰退',
            'low_risk_explanation': '工业生产增长表示经济扩张',
            'unit': '%',
            'typical_range': '0% - 5%',
            'crisis_threshold': '< -2%'
        },
        'UNRATE': {
            'name': '失业率',
            'description': '失业人口占总劳动力的比例',
            'high_risk_explanation': '失业率高表示经济衰退，就业困难',
            'low_risk_explanation': '失业率低表示经济健康，就业充分',
            'unit': '%',
            'typical_range': '3% - 6%',
            'crisis_threshold': '> 7%'
        },
        'CPIAUCSL': {
            'name': '消费者价格指数',
            'description': '衡量通胀水平的指标',
            'high_risk_explanation': '通胀过高会侵蚀购买力，央行可能加息',
            'low_risk_explanation': '适度通胀有利于经济增长',
            'unit': '%',
            'typical_range': '1% - 3%',
            'crisis_threshold': '> 5%'
        },
        'FEDFUNDS': {
            'name': '联邦基金利率',
            'description': '美联储设定的短期利率，影响整个经济',
            'high_risk_explanation': '利率过高会抑制经济增长',
            'low_risk_explanation': '适度利率有利于经济平衡',
            'unit': '%',
            'typical_range': '0% - 5%',
            'crisis_threshold': '> 6%'
        },
        'HOUST': {
            'name': '新屋开工数',
            'description': '衡量房地产市场的活跃程度',
            'high_risk_explanation': '新屋开工下降表示房地产市场疲软',
            'low_risk_explanation': '新屋开工增长表示房地产市场活跃',
            'unit': '千套',
            'typical_range': '1000 - 2000',
            'crisis_threshold': '< 800'
        },
        'UMCSENT': {
            'name': '密歇根消费者信心指数',
            'description': '衡量消费者对未来经济的信心',
            'high_risk_explanation': '信心低表示消费者对未来悲观，消费减少',
            'low_risk_explanation': '信心高表示消费者对未来乐观，消费增加',
            'unit': '指数',
            'typical_range': '80 - 100',
            'crisis_threshold': '< 70'
        },
        'SOFR20DMA_MINUS_DTB3': {
            'name': 'SOFR(20日均值)-3个月国债利差',
            'description': '有担保隔夜融资利率与3个月国债利率的差异，反映短期流动性状况',
            'high_risk_explanation': '利差扩大表示短期融资成本上升，流动性紧张',
            'low_risk_explanation': '利差较小表示短期融资成本低，流动性充足',
            'unit': '%',
            'typical_range': '0.1% - 0.5%',
            'crisis_threshold': '> 0.5%'
        },
        'PAYEMS': {
            'name': '非农就业人数',
            'description': '美国非农业部门就业人数的年度变化率',
            'high_risk_explanation': '就业增长放缓表示经济疲软，失业风险上升',
            'low_risk_explanation': '就业稳定增长表示经济健康，就业市场强劲',
            'unit': '%',
            'typical_range': '1% - 3%',
            'crisis_threshold': '< 0.5%'
        },
        'CSUSHPINSA': {
            'name': '房价指数: Case-Shiller 20城',
            'description': '美国20个主要城市的房价指数年度变化率',
            'high_risk_explanation': '房价下跌表示房地产市场疲软，可能引发债务危机',
            'low_risk_explanation': '房价稳定增长表示房地产市场健康',
            'unit': '%',
            'typical_range': '2% - 8%',
            'crisis_threshold': '< 0%'
        },
        'PERMIT': {
            'name': '住宅建筑许可',
            'description': '新住宅建筑许可数量的年度变化率',
            'high_risk_explanation': '建筑许可大幅下降表示房地产市场萎缩',
            'low_risk_explanation': '建筑许可稳定表示房地产市场活跃',
            'unit': '%',
            'typical_range': '-5% - 10%',
            'crisis_threshold': '< -15%'
        },
        'MORTGAGE30US': {
            'name': '30年期按揭利率',
            'description': '30年期固定利率抵押贷款的平均利率',
            'high_risk_explanation': '利率过高会抑制购房需求，影响房地产市场',
            'low_risk_explanation': '适度利率有利于房地产市场发展',
            'unit': '%',
            'typical_range': '3% - 7%',
            'crisis_threshold': '> 8%'
        },
        'CPN3M': {
            'name': '3个月商业票据利率',
            'description': '3个月期商业票据的平均利率，反映企业短期融资成本',
            'high_risk_explanation': '利率过高会增加企业融资成本，影响经营',
            'low_risk_explanation': '适度利率有利于企业融资和发展',
            'unit': '%',
            'typical_range': '1% - 5%',
            'crisis_threshold': '> 6%'
        },
        'TOTLL': {
            'name': '总贷款和租赁',
            'description': '银行总贷款和租赁业务的年度变化率',
            'high_risk_explanation': '贷款增长放缓表示银行放贷谨慎，经济疲软',
            'low_risk_explanation': '贷款稳定增长表示银行放贷活跃，经济健康',
            'unit': '%',
            'typical_range': '3% - 8%',
            'crisis_threshold': '< 1%'
        },
        'DRTSCILM': {
            'name': '银行贷款标准-大中企C&I收紧净比例',
            'description': '银行对大中型企业商业和工业贷款收紧标准的净比例',
            'high_risk_explanation': '收紧比例高表示银行风险偏好下降，信贷紧缩',
            'low_risk_explanation': '收紧比例低表示银行风险偏好正常，信贷宽松',
            'unit': '%',
            'typical_range': '0% - 20%',
            'crisis_threshold': '> 30%'
        },
        'NCBDBIQ027S': {
            'name': '企业债/GDP（新）',
            'description': '企业债务总额占GDP的百分比',
            'high_risk_explanation': '企业债务过高会增加违约风险，影响金融稳定',
            'low_risk_explanation': '企业债务适中表示企业杠杆合理',
            'unit': '%',
            'typical_range': '20% - 30%',
            'crisis_threshold': '> 35%'
        },
        'CORPDEBT_GDP_PCT': {
            'name': '企业债/GDP（旧）',
            'description': '企业债务总额占GDP的百分比（旧计算方法）',
            'high_risk_explanation': '企业债务过高会增加违约风险，影响金融稳定',
            'low_risk_explanation': '企业债务适中表示企业杠杆合理',
            'unit': '%',
            'typical_range': '18% - 28%',
            'crisis_threshold': '> 32%'
        },
        'THREEFYTP10': {
            'name': '期限溢价-10年期Kim-Wright代理',
            'description': '10年期国债的期限溢价，反映长期利率风险',
            'high_risk_explanation': '期限溢价过高表示长期利率风险上升',
            'low_risk_explanation': '期限溢价适中表示长期利率风险可控',
            'unit': '%',
            'typical_range': '0.5% - 2.0%',
            'crisis_threshold': '> 3.0%'
        },
        'MANEMP': {
            'name': '制造业就业',
            'description': '制造业就业人数的年度变化率',
            'high_risk_explanation': '制造业就业下降表示制造业萎缩，经济衰退',
            'low_risk_explanation': '制造业就业稳定表示制造业健康',
            'unit': '%',
            'typical_range': '-2% - 2%',
            'crisis_threshold': '< -3%'
        },
        'NEWORDER': {
            'name': '制造业新订单-非国防资本货不含飞机',
            'description': '制造业新订单的年度变化率（排除国防和飞机）',
            'high_risk_explanation': '新订单下降表示制造业需求疲软',
            'low_risk_explanation': '新订单增长表示制造业需求旺盛',
            'unit': '%',
            'typical_range': '-5% - 10%',
            'crisis_threshold': '< -10%'
        },
        'AWHMAN': {
            'name': '制造业平均周工时',
            'description': '制造业工人的平均每周工作小时数',
            'high_risk_explanation': '工时减少表示制造业活动放缓',
            'low_risk_explanation': '工时稳定表示制造业活动正常',
            'unit': '小时',
            'typical_range': '40 - 42',
            'crisis_threshold': '< 39'
        },
        'DGS10': {
            'name': '10年期国债利率',
            'description': '10年期美国国债的收益率',
            'high_risk_explanation': '利率过高会抑制经济增长，增加债务负担',
            'low_risk_explanation': '适度利率有利于经济平衡发展',
            'unit': '%',
            'typical_range': '2% - 6%',
            'crisis_threshold': '> 7%'
        },
        'SOFR': {
            'name': 'SOFR',
            'description': '有担保隔夜融资利率，反映短期资金成本',
            'high_risk_explanation': 'SOFR过高会增加短期融资成本',
            'low_risk_explanation': 'SOFR适中有利于短期融资',
            'unit': '%',
            'typical_range': '0% - 5%',
            'crisis_threshold': '> 6%'
        },
        'DTB3': {
            'name': '3个月国债利率',
            'description': '3个月期美国国债的收益率',
            'high_risk_explanation': '利率过高会增加短期融资成本',
            'low_risk_explanation': '适度利率有利于短期融资',
            'unit': '%',
            'typical_range': '0% - 5%',
            'crisis_threshold': '> 6%'
        },
        'WALCL': {
            'name': '美联储总资产',
            'description': '美联储资产负债表总资产的年度变化率',
            'high_risk_explanation': '资产大幅收缩表示货币政策收紧',
            'low_risk_explanation': '资产稳定表示货币政策适中',
            'unit': '%',
            'typical_range': '-5% - 10%',
            'crisis_threshold': '< -10%'
        },
        'TOTALSA': {
            'name': '消费者信贷',
            'description': '消费者信贷总额的年度变化率',
            'high_risk_explanation': '信贷增长过快可能引发债务风险',
            'low_risk_explanation': '信贷适度增长有利于消费',
            'unit': '%',
            'typical_range': '3% - 8%',
            'crisis_threshold': '> 12%'
        },
        'TDSP': {
            'name': '家庭债务偿付比率',
            'description': '家庭债务偿付占可支配收入的比例',
            'high_risk_explanation': '比率过高表示家庭债务负担过重',
            'low_risk_explanation': '比率适中表示家庭债务负担合理',
            'unit': '%',
            'typical_range': '8% - 12%',
            'crisis_threshold': '> 15%'
        },
        'TOTRESNS': {
            'name': '银行准备金',
            'description': '银行准备金的年度变化率',
            'high_risk_explanation': '准备金大幅下降可能影响银行流动性',
            'low_risk_explanation': '准备金稳定表示银行流动性充足',
            'unit': '%',
            'typical_range': '-5% - 5%',
            'crisis_threshold': '< -10%'
        },
        'IC4WSA': {
            'name': '初请失业金4周均值',
            'description': '初次申请失业救济金的4周移动平均值',
            'high_risk_explanation': '申请人数过多表示就业市场疲软',
            'low_risk_explanation': '申请人数适中表示就业市场健康',
            'unit': '人',
            'typical_range': '200,000 - 300,000',
            'crisis_threshold': '> 400,000'
        },
        'DTWEXBGS': {
            'name': '贸易加权美元',
            'description': '美元对主要贸易伙伴货币的加权汇率年度变化率',
            'high_risk_explanation': '美元过强会削弱出口竞争力',
            'low_risk_explanation': '美元适中有利于贸易平衡',
            'unit': '%',
            'typical_range': '-5% - 5%',
            'crisis_threshold': '> 10%'
        },
        'STLFSI3': {
            'name': '圣路易斯金融压力',
            'description': '圣路易斯联邦储备银行的金融压力指数',
            'high_risk_explanation': '压力指数高表示金融市场压力大',
            'low_risk_explanation': '压力指数低表示金融市场稳定',
            'unit': '指数',
            'typical_range': '-2.0 - 2.0',
            'crisis_threshold': '> 3.0'
        },
        'DRSFRMACBS': {
            'name': '房贷违约率',
            'description': '住房抵押贷款违约率',
            'high_risk_explanation': '违约率高表示房地产市场风险上升',
            'low_risk_explanation': '违约率低表示房地产市场健康',
            'unit': '%',
            'typical_range': '1% - 3%',
            'crisis_threshold': '> 5%'
        }
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# 📚 FRED指标详细解释报告\n\n")
        f.write(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
        
        f.write("## 📖 使用说明\n\n")
        f.write("本报告为每个指标提供通俗易懂的解释，帮助普通投资者理解经济指标的含义。\n\n")
        f.write("每个指标包含：\n")
        f.write("- **指标含义**：这个指标代表什么\n")
        f.write("- **高低风险解释**：为什么高/低值表示风险\n")
        f.write("- **典型范围**：正常情况下的数值范围\n")
        f.write("- **危机阈值**：达到什么水平需要警惕\n")
        f.write("- **当前状态**：与历史数据的对比\n\n")
        
        f.write("## 📊 指标详细解释\n\n")
        
        # 按风险等级分组
        high_risk = [i for i in processed_indicators if i['risk_score'] >= 80]
        medium_risk = [i for i in processed_indicators if 60 <= i['risk_score'] < 80]
        low_risk = [i for i in processed_indicators if i['risk_score'] < 60]
        
        # 高风险指标
        if high_risk:
            f.write("### 🔴 高风险指标\n\n")
            for indicator in high_risk:
                series_id = indicator['series_id']
                explanation = indicator_explanations.get(series_id, {})
                
                f.write(f"#### {explanation.get('name', indicator['name'])} ({series_id})\n\n")
                f.write(f"**指标含义**: {explanation.get('description', '暂无详细解释')}\n\n")
                f.write(f"**当前值**: {indicator['current_value']:.4f} {explanation.get('unit', '')}\n")
                f.write(f"**风险评分**: {indicator['risk_score']:.1f}/100 (🔴 高风险)\n\n")
                
                f.write(f"**为什么是高风险**: {explanation.get('high_risk_explanation', '当前值偏离正常范围')}\n\n")
                f.write(f"**典型范围**: {explanation.get('typical_range', '暂无数据')}\n")
                f.write(f"**危机阈值**: {explanation.get('crisis_threshold', '暂无数据')}\n\n")
                
                # 添加历史对比
                if 'benchmark_value' in indicator:
                    f.write(f"**历史对比**: 当前值 {indicator['current_value']:.4f} vs 基准值 {indicator['benchmark_value']:.4f}\n")
                    if indicator['current_value'] > indicator['benchmark_value']:
                        f.write("当前值高于历史基准，风险上升\n\n")
                    else:
                        f.write("当前值低于历史基准，风险相对较低\n\n")
                
                f.write("---\n\n")
        
        # 中风险指标
        if medium_risk:
            f.write("### 🟡 中风险指标\n\n")
            for indicator in medium_risk:
                series_id = indicator['series_id']
                explanation = indicator_explanations.get(series_id, {})
                
                f.write(f"#### {explanation.get('name', indicator['name'])} ({series_id})\n\n")
                f.write(f"**指标含义**: {explanation.get('description', '暂无详细解释')}\n\n")
                f.write(f"**当前值**: {indicator['current_value']:.4f} {explanation.get('unit', '')}\n")
                f.write(f"**风险评分**: {indicator['risk_score']:.1f}/100 (🟡 中风险)\n\n")
                
                f.write(f"**风险解释**: {explanation.get('high_risk_explanation', '当前值略偏离正常范围')}\n\n")
                f.write(f"**典型范围**: {explanation.get('typical_range', '暂无数据')}\n")
                f.write(f"**危机阈值**: {explanation.get('crisis_threshold', '暂无数据')}\n\n")
                
                if 'benchmark_value' in indicator:
                    f.write(f"**历史对比**: 当前值 {indicator['current_value']:.4f} vs 基准值 {indicator['benchmark_value']:.4f}\n\n")
                
                f.write("---\n\n")
        
        # 低风险指标
        if low_risk:
            f.write("### 🟢 低风险指标\n\n")
            for indicator in low_risk:
                series_id = indicator['series_id']
                explanation = indicator_explanations.get(series_id, {})
                
                f.write(f"#### {explanation.get('name', indicator['name'])} ({series_id})\n\n")
                f.write(f"**指标含义**: {explanation.get('description', '暂无详细解释')}\n\n")
                f.write(f"**当前值**: {indicator['current_value']:.4f} {explanation.get('unit', '')}\n")
                f.write(f"**风险评分**: {indicator['risk_score']:.1f}/100 (🟢 低风险)\n\n")
                
                f.write(f"**为什么是低风险**: {explanation.get('low_risk_explanation', '当前值在正常范围内')}\n\n")
                f.write(f"**典型范围**: {explanation.get('typical_range', '暂无数据')}\n")
                f.write(f"**危机阈值**: {explanation.get('crisis_threshold', '暂无数据')}\n\n")
                
                if 'benchmark_value' in indicator:
                    f.write(f"**历史对比**: 当前值 {indicator['current_value']:.4f} vs 基准值 {indicator['benchmark_value']:.4f}\n\n")
                
                f.write("---\n\n")
        
        f.write("## 📝 总结\n\n")
        f.write("本报告基于FRED官方数据，通过对比历史危机期间的数据来评估当前风险。\n\n")
        f.write("**重要提醒**:\n")
        f.write("- 这些指标仅供参考，不构成投资建议\n")
        f.write("- 经济指标存在滞后性，需要结合其他信息综合判断\n")
        f.write("- 市场情况复杂多变，单一指标不能完全预测未来\n")
        f.write("- 建议咨询专业投资顾问获得个性化建议\n\n")
        
        f.write("**数据来源**: FRED (Federal Reserve Economic Data)\n")
        f.write("**报告生成**: FRED危机预警监控系统\n")
        f.write("**联系方式**: jiangx@gmail.com\n\n")
        
        f.write("---\n")
        f.write(f"*报告生成时间: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}*\n")

def generate_report_with_images():
    """生成带图片的危机预警报告"""
    print("🚨 FRED 危机预警监控系统启动...")
    print("=" * 80)
    
    # 首先运行数据管道
    run_data_pipeline()

    # 加载配置
    config_path = BASE / "config" / "crisis_indicators.yaml"
    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return
    
    config = load_yaml_config(config_path)
    indicators = config.get('indicators', [])
    scoring_config = config.get('scoring', {})
    bands = scoring_config.get('bands', {'low': 40, 'med': 60, 'high': 80})
    print(f"📊 加载了 {len(indicators)} 个指标")
    
    # 计算真实FRED数据评分
    result = calculate_real_fred_scores(indicators, scoring_config)
    if len(result) == 4:
        group_scores, total_score, processed_indicators, tension_metrics = result
    else:
        # 向后兼容：如果没有张力指标，创建默认值
        group_scores, total_score, processed_indicators = result
        tension_metrics = {
            'score_state': 0.0,
            'score_trigger': 0.0,
            'index_tension': 0.0,
            'tension_analysis': '数据不足，无法计算系统张力。'
        }
    
    # v2.0: 为每个指标添加风险等级和趋势
    for indicator in processed_indicators:
        indicator['risk_level'] = level_from_score(indicator['risk_score'], bands)
        # 计算趋势箭头（需要时间序列数据）
        series_id = indicator.get('series_id', '')
        try:
            ts = compose_series(series_id)
            if ts is None:
                ts = fetch_series(series_id)
            if ts is not None and not ts.empty:
                indicator['trend'] = calculate_trend(ts, indicator['risk_score'])
            else:
                indicator['trend'] = "→"
        except:
            indicator['trend'] = "→"
    
    # v2.0: 生成宏观叙事
    macro_narrative = generate_macro_narrative(processed_indicators, group_scores, total_score, scoring_config)
    
    # 生成报告（使用JST时区）
    now = datetime.now(JST)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    display_time = now.strftime("%Y年%m月%d日 %H:%M:%S JST")
    output_dir = BASE / "outputs" / "crisis_monitor"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 生成图片
    print("📊 生成图表...")
    image_paths = {}
    
    # 生成总体概览图表
    summary_chart_path = generate_summary_chart(group_scores, total_score, output_dir)
    if summary_chart_path:
        image_paths['summary'] = summary_chart_path
    
    # 生成主要指标图表 - 使用完整的viz.py功能
    main_chart_paths = []
    
    # 导入图表生成模块
    try:
        from scripts.viz import save_indicator_plot
        viz_available = True
        print("✅ 成功导入viz模块")
    except ImportError:
        print("⚠️ 无法导入viz模块，使用简化图表")
        viz_available = False
    
    # 加载危机期间配置
    crisis_config_path = BASE / "config" / "crisis_periods.yaml"
    crisis_config = load_yaml_config(crisis_config_path)
    crisis_periods = crisis_config.get('crises', [])
    
    # 为所有指标生成完整图表
    for i, indicator in enumerate(processed_indicators):
        try:
            series_id = indicator['series_id']
            chart_path = output_dir / "figures" / f"{series_id}_latest.png"
            
            if viz_available:
                # 使用完整的图表生成功能
                from scripts.fred_http import series_observations
                from scripts.clean_utils import parse_numeric_series
                
                ts = None
                
                # 优先使用预计算的数据（YoY或比率）
                if series_id == 'NCBDBIQ027S':
                    # 使用预计算的企业债/GDP比率数据
                    try:
                        ratio_file = pathlib.Path("data/series/CORPORATE_DEBT_GDP_RATIO.csv")
                        if ratio_file.exists():
                            ratio_df = pd.read_csv(ratio_file)
                            ratio_df['date'] = pd.to_datetime(ratio_df['date'])
                            ratio_df = ratio_df.set_index('date')
                            ts = parse_numeric_series(ratio_df['value']).dropna()
                            print(f"📊 使用企业债/GDP比率数据: {series_id}")
                    except Exception as e:
                        print(f"⚠️ 企业债/GDP比率数据读取失败: {e}")
                
                # 处理合成指标
                elif series_id in ['CP_MINUS_DTB3', 'SOFR20DMA_MINUS_DTB3', 'CORPDEBT_GDP_PCT', 'RESERVES_ASSETS_PCT', 'RESERVES_DEPOSITS_PCT']:
                    # 使用预计算的合成指标数据
                    try:
                        synthetic_file = pathlib.Path(f"data/series/{series_id}.csv")
                        if synthetic_file.exists():
                            synthetic_df = pd.read_csv(synthetic_file)
                            synthetic_df['date'] = pd.to_datetime(synthetic_df['date'])
                            synthetic_df = synthetic_df.set_index('date')
                            ts = synthetic_df['value'].dropna()
                            print(f"📁 使用预计算合成指标数据: {series_id}")
                    except Exception as e:
                        print(f"⚠️ 合成指标数据读取失败: {e}")
                
                elif series_id in ['PAYEMS', 'INDPRO', 'GDP', 'NEWORDER', 'CSUSHPINSA', 'TOTALSA', 'TOTLL', 'MANEMP', 'WALCL', 'DTWEXBGS', 'PERMIT', 'TOTRESNS', 'USD_NET_LIQUIDITY']:
                    # 使用预计算的YoY数据
                    try:
                        yoy_file = pathlib.Path(f"data/series/{series_id}_YOY.csv")
                        if yoy_file.exists():
                            yoy_df = pd.read_csv(yoy_file)
                            yoy_df['date'] = pd.to_datetime(yoy_df['date'])
                            yoy_df = yoy_df.set_index('date')
                            ts = parse_numeric_series(yoy_df['yoy_pct']).dropna()
                            print(f"📊 使用YoY数据: {series_id}")
                    except Exception as e:
                        print(f"⚠️ YoY数据读取失败: {e}")
                
                # 黄金见顶判断模型、权益周期监控和长期估值锚合成指标
                elif series_id in ['US_REAL_RATE_10Y', 'GOLD_REAL_RATE_DIFF', 'SP500_PROFIT_DIVERGENCE', 'USD_NET_LIQUIDITY', 'BUFFETT_INDICATOR']:
                    # 使用预计算的合成指标数据
                    try:
                        # v3.0: 检查市场体制指标和其他合成指标
                        synthetic_file = pathlib.Path(f"data/series/{series_id}.csv")
                        if synthetic_file.exists():
                            synthetic_df = pd.read_csv(synthetic_file, index_col=0, parse_dates=True)
                            if len(synthetic_df.columns) == 1:
                                ts = parse_numeric_series(synthetic_df.iloc[:, 0]).dropna()
                            else:
                                # 优先使用 'value' 列，否则使用第一列
                                if 'value' in synthetic_df.columns:
                                    ts = parse_numeric_series(synthetic_df['value']).dropna()
                                else:
                                    ts = parse_numeric_series(synthetic_df.iloc[:, 0]).dropna()
                            print(f"📁 使用预计算合成指标数据: {series_id}")
                    except Exception as e:
                        print(f"⚠️ 合成指标数据读取失败: {e}")
                
                # 如果没有预计算数据，使用原始数据
                if ts is None or ts.empty:
                    local_data_path = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
                    
                    if local_data_path.exists():
                        try:
                            print(f"📁 使用本地原始数据: {series_id}")
                            df = pd.read_csv(local_data_path, index_col=0, parse_dates=True)
                            if len(df.columns) > 0:
                                ts = parse_numeric_series(df.iloc[:, 0])
                                ts = ts.dropna()
                        except Exception as e:
                            print(f"⚠️ 本地数据读取失败: {e}")
                    
                    # 如果本地数据不可用，使用API
                    if ts is None or ts.empty:
                        print(f"🌐 使用API获取数据: {series_id}")
                        try:
                            data_response = series_observations(series_id)
                            if data_response and 'observations' in data_response:
                                observations = data_response.get('observations', [])
                                if observations:
                                    df = pd.DataFrame(observations)
                                    df['date'] = pd.to_datetime(df['date'])
                                    df = df.set_index('date')
                                    ts = parse_numeric_series(df['value'])
                                    ts = ts.dropna()
                                else:
                                    print(f"⚠️ 无观测数据: {series_id}")
                            else:
                                print(f"⚠️ 无法获取数据: {series_id}")
                        except Exception as e:
                            print(f"⚠️ API获取数据失败: {series_id} - {e}")
                
                # 如果有数据，生成图表
                if ts is not None and not ts.empty:
                    try:
                        # 计算危机统计
                        crisis_stats = calculate_crisis_stats(ts, crisis_periods)
                        
                        # 生成完整图表（包含危机期间阴影、历史均值线等）
                        save_indicator_plot(
                            ts=ts,
                            title=indicator['name'],
                            unit=indicator.get('unit', ''),
                            crises=crisis_periods,
                            crisis_stats=crisis_stats,
                            out_path=chart_path,
                            show_ma=[6, 12],
                            annotate_latest=True
                        )
                        main_chart_paths.append(f"figures/{series_id}_latest.png")
                        print(f"✅ 生成图表: {series_id}")
                    except Exception as e:
                        print(f"⚠️ 图表生成失败: {series_id} - {e}")
                else:
                    print(f"⚠️ 无数据生成图表: {series_id}")
            else:
                # 简化图表
                chart_path = generate_indicator_chart(
                    indicator['name'], 
                    indicator['current_value'], 
                    indicator['benchmark_value'], 
                    indicator['risk_score'], 
                    output_dir
                )
                if chart_path:
                    main_chart_paths.append(chart_path)
                    
        except Exception as e:
            print(f"❌ 生成图表失败 {indicator.get('series_id', 'Unknown')}: {e}")
            continue

    if main_chart_paths:
        image_paths['main_indicators'] = main_chart_paths[0]  # 使用第一个作为代表
    
    # 生成详细解释Markdown报告
    detailed_md_path = output_dir / f"crisis_report_detailed_{timestamp}.md"
    generate_detailed_explanation_report(processed_indicators, detailed_md_path, timestamp)
    
    # 生成Markdown报告 - 优化排版格式
    md_path = output_dir / f"crisis_report_{timestamp}.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# 🚨 FRED 宏观金融危机预警监控报告\n\n")
        f.write(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n")
        f.write(f"**系统版本**: v2.4 (Structural Fragility Floor - 结构性脆弱底座)\n\n")
        
        # v2.4: 双轨风险面板（在报告最前面）
        f.write("## ⚡ v2.4 双轨风险面板 (System Tension Dashboard)\n\n")
        
        score_state = tension_metrics.get('score_state', 0.0)
        score_trigger = tension_metrics.get('score_trigger', 0.0)
        # v2.4: 使用最终风险分（考虑结构性底座）
        final_risk_score = tension_metrics.get('final_risk_score', tension_metrics.get('index_tension', 0.0))
        structural_floor = tension_metrics.get('structural_floor', 0.0)
        index_tension = tension_metrics.get('index_tension', 0.0)
        tension_analysis = tension_metrics.get('tension_analysis', '数据不足')
        
        # Markdown格式的双轨面板
        f.write("### 📉 回报不对称性 (Valuation Risk - State)\n\n")
        f.write(f"**得分**: {score_state:.1f} / 100\n\n")
        f.write("**含义**：资产是否昂贵？未来长期回报是否受限？\n\n")
        f.write("**策略**：分数越高，越应降低预期收益，而非立刻卖出。\n\n")
        f.write("---\n\n")
        f.write("### 💥 崩盘触发概率 (Trigger Risk)\n\n")
        f.write(f"**得分**: {score_trigger:.1f} / 100\n\n")
        f.write("**含义**：流动性/信用/情绪是否在恶化？\n\n")
        f.write("**策略**：分数越高，越应增加对冲或现金。\n\n")
        f.write("---\n\n")
        f.write("### ⚡ 系统张力指数 (System Tension)\n\n")
        f.write(f"**指数**: {final_risk_score:.1f} / 100\n\n")
        
        # v2.3: 显示 Momentum 和 Convexity
        trigger_momentum = tension_metrics.get('trigger_momentum', 0.0)
        convexity_factor = tension_metrics.get('convexity_factor', 1.0)
        
        if trigger_momentum > 0:
            momentum_text = f"+{trigger_momentum:.1f} (恶化 ⚠️)"
        else:
            momentum_text = f"{trigger_momentum:.1f} (改善)"
        
        f.write(f"**Trigger Momentum**: {momentum_text}\n\n")
        f.write(f"**Convexity Multiplier**: {convexity_factor:.2f}x\n\n")
        
        # v2.4: 显示结构性底座
        if structural_floor > 0:
            if final_risk_score == structural_floor:
                f.write(f"**🛡️ 结构性底座**: {structural_floor:.1f} (已激活) ⚠️\n\n")
                f.write(f"*原因：State ({tension_metrics.get('score_state', 0.0):.1f}) 过高，系统强制锁定最低风险分。*\n\n")
            else:
                f.write(f"**🛡️ 结构性底座**: {structural_floor:.1f} (未激活)\n\n")
        
        f.write(f"**分析**: {tension_analysis}\n\n")
        f.write(f"*v2.4 算法：当 State > 40 时，系统进入\"易碎模式\" (Fragile Mode)，Trigger 的微小恶化会被凸性放大 {convexity_factor:.1f} 倍。同时，高估值会触发结构性底座，防止风险低估。*\n\n")
        f.write("---\n\n")
        
        # 核心方法论声明（V2.4更新）
        f.write("### ℹ️ 如何阅读本报告 (V2.4)\n\n")
        f.write("本系统采用**\"三层指标体系\"**来区分\"贵\"与\"险\"：\n\n")
        f.write("1. **状态指标 (State)**：如巴菲特指标、利润背离。它们只告诉你\"资产贵不贵\"，**不直接触发危机预警**。\n\n")
        f.write("2. **触发指标 (Trigger)**：如流动性、信用利差、FCI。它们决定\"泡沫会不会破\"，是危机爆发的扳机。\n\n")
        f.write("3. **约束指标 (Constraint)**：如实际利率、财政赤字。它们是资产定价的宏观边界。\n\n")
        f.write("**系统张力指数** = 基础张力 + (动量风险 × 凸性放大)。\n\n")
        f.write("        * **基础张力** = (State得分 × Trigger得分) / 100\n")
        f.write("        * **动量风险** = Trigger当前值 - Trigger 13周均值（仅当恶化时>0）\n")
        f.write("        * **凸性放大** = 当State>40时，使用指数函数放大动量风险\n\n")
        f.write("        高估值+触发恶化 = 极度危险（动量被大幅放大）。\n\n")
        f.write("*当前总分较低（极低风险）是因为\"触发指标\"依然健康，尽管\"状态指标\"已经报警。请据此区分长期配置风险与短期交易风险。*\n\n")
        f.write("---\n\n")
        
        f.write("## 📋 报告说明\n\n")
        f.write("本报告基于FRED宏观指标，将当前值与历史危机期间基准值比较，以评估风险。\n\n")
        f.write("【数据由人采集和处理，请批判看待这些数据，欢迎email jiangx@gmail.com 任何问题讨论】\n\n")
        f.write("风险评分范围 0-100：50 为中性，越高越危险（除非指标设定为'越低越危险'）。\n\n")
        f.write("采用分组加权评分：先计算各组平均分，再按权重合成总分。\n\n")
        f.write("总分 = ∑(分组平均分 × 分组权重)，分组权重归一处理后合成。\n\n")
        f.write("过期数据处理：月频数据>60天、季频数据>120天标记⚠️，过期数据权重×0.9。\n\n")
        f.write(f"颜色分段：0–{bands['low']-1} 🔵 极低，{bands['low']}–{bands['med']-1} 🟢 低，{bands['med']}–{bands['high']-1} 🟡 中，{bands['high']}–100 🔴 高；50 为中性。\n\n")
        
        f.write("## 📊 执行摘要\n\n")
        f.write(f"- **总指标数**: {len(processed_indicators)}\n")
        f.write(f"- **高风险指标**: {len([i for i in processed_indicators if i['risk_score'] >= bands['high']])} 个\n")
        f.write(f"- **中风险指标**: {len([i for i in processed_indicators if bands['med'] <= i['risk_score'] < bands['high']])} 个\n")
        f.write(f"- **低风险指标**: {len([i for i in processed_indicators if bands['low'] <= i['risk_score'] < bands['med']])} 个\n")
        f.write(f"- **极低风险指标**: {len([i for i in processed_indicators if i['risk_score'] < bands['low']])} 个\n\n")
        
        f.write("### 🎯 风险等级说明\n\n")
        f.write(f"- 🔴 **高风险** ({bands['high']}-100分): 当前值显著偏离历史危机水平\n")
        f.write(f"- 🟡 **中风险** ({bands['med']}-{bands['high']-1}分): 当前值略高于历史危机水平\n")
        f.write(f"- 🟢 **低风险** ({bands['low']}-{bands['med']-1}分): 当前值接近或低于历史危机水平\n")
        f.write(f"- 🔵 **极低风险** (0-{bands['low']-1}分): 当前值远低于历史危机水平\n\n")
        
        # v2.0: 添加宏观叙事部分
        f.write("## 📖 宏观叙事分析 (v2.0)\n\n")
        f.write(macro_narrative)
        f.write("\n\n")
        
        # v3.0: 添加市场体制雷达诊断
        try:
            market_md, market_html = generate_market_regime_diagnosis(processed_indicators)
            f.write(market_md)
            f.write("\n")
        except Exception as e:
            print(f"⚠️ 市场体制诊断生成失败: {e}")
            f.write("### 🧭 市场体制雷达\n\n*市场数据暂不可用*\n\n")
        
        # 添加美股泡沫深度诊断（三段式逻辑）
        bubble_md, bubble_html = generate_bubble_diagnosis(processed_indicators)
        f.write(bubble_md)
        f.write("\n")
        
        # 添加黄金市场深度诊断
        gold_html, gold_md = generate_gold_diagnosis(processed_indicators)
        f.write(gold_md)
        f.write("\n\n")
        
        # 保存gold_html和gold_md供后续HTML生成使用
        # 注意：这里我们需要在函数外部保存这些值，但由于作用域限制，我们在HTML生成时重新生成
        
        f.write("## 📈 详细指标分析\n\n")
        
        # v2.0: 按指标类型重组报告结构
        # 1. 先行指标 (Leading Indicators)
        leading_indicators = [i for i in processed_indicators 
                             if i.get('type') == 'leading' or 
                             i.get('series_id', '') in ['T10Y3M', 'T10Y2Y', 'SAHMREALTIME', 'KCFSI', 'T5YIE']]
        
        if leading_indicators:
            f.write("### 🚨 先行指标 (早期预警系统)\n\n")
            f.write("| 指标名称 | 当前值 | 风险评分 | 趋势 | 说明 |\n")
            f.write("|---------|--------|----------|------|------|\n")
            for indicator in sorted(leading_indicators, key=lambda x: x['risk_score'], reverse=True):
                trend = indicator.get('trend', '→')
                f.write(f"| {indicator['name']} | {indicator['current_value']:.2f} | "
                       f"{indicator['risk_score']:.1f} | {trend} | {indicator.get('plain_explainer', '')[:50]}... |\n")
            f.write("\n")
        
        # 2. 流动性条件 (Liquidity Conditions)
        liquidity_indicators = [i for i in processed_indicators 
                               if i.get('group') == 'liquidity' or
                               i.get('series_id', '') in ['RRPONTSYD', 'WTREGEN', 'M2SL']]
        
        if liquidity_indicators:
            f.write("### 💧 流动性条件 (市场燃料)\n\n")
            f.write("| 指标名称 | 当前值 | 风险评分 | 趋势 | 说明 |\n")
            f.write("|---------|--------|----------|------|------|\n")
            for indicator in sorted(liquidity_indicators, key=lambda x: x['risk_score'], reverse=True):
                trend = indicator.get('trend', '→')
                f.write(f"| {indicator['name']} | {indicator['current_value']:.2f} | "
                       f"{indicator['risk_score']:.1f} | {trend} | {indicator.get('plain_explainer', '')[:50]}... |\n")
            f.write("\n")
        
        # 3. 按风险等级分组显示所有指标（保留原有结构）
        high_risk_indicators = []
        medium_risk_indicators = []
        low_risk_indicators = []
        very_low_risk_indicators = []
        
        for indicator in processed_indicators:
            score = indicator['risk_score']
            if score >= bands['high']:
                high_risk_indicators.append(indicator)
            elif score >= bands['med']:
                medium_risk_indicators.append(indicator)
            elif score >= bands['low']:
                low_risk_indicators.append(indicator)
            else:
                very_low_risk_indicators.append(indicator)
        
        # 高风险指标
        if high_risk_indicators:
            f.write("### 🔴 高风险指标\n\n")
            for i, indicator in enumerate(high_risk_indicators, 1):
                trend = indicator.get('trend', '→')
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']}) {trend}\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🔴 高风险) | **趋势**: {trend}\n")
                f.write(f"- **解释**: {indicator.get('plain_explainer', '无解释')}\n\n")
                f.write(f"![{indicator['series_id']}](figures/{indicator['series_id']}_latest.png)\n\n")
        
        # 中风险指标
        if medium_risk_indicators:
            f.write("### 🟡 中风险指标\n\n")
            for i, indicator in enumerate(medium_risk_indicators, 1):
                trend = indicator.get('trend', '→')
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']}) {trend}\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🟡 中风险) | **趋势**: {trend}\n")
                f.write(f"- **解释**: {indicator.get('plain_explainer', '无解释')}\n\n")
                f.write(f"![{indicator['series_id']}](figures/{indicator['series_id']}_latest.png)\n\n")
        
        # 低风险指标
        if low_risk_indicators:
            f.write("### 🟢 低风险指标\n\n")
            for i, indicator in enumerate(low_risk_indicators, 1):
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']})\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🟢 低风险)\n")
                f.write(f"- **解释**: {indicator.get('plain_explainer', '无解释')}\n\n")
                f.write(f"![{indicator['series_id']}](figures/{indicator['series_id']}_latest.png)\n\n")
        
        # 极低风险指标
        if very_low_risk_indicators:
            f.write("### 🔵 极低风险指标\n\n")
            for i, indicator in enumerate(very_low_risk_indicators, 1):
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']})\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🔵 极低风险)\n")
                f.write(f"- **解释**: {indicator.get('plain_explainer', '无解释')}\n\n")
                f.write(f"![{indicator['series_id']}](figures/{indicator['series_id']}_latest.png)\n\n")
        
        # 添加总体概览图表
        if summary_chart_path:
            f.write("## 📊 总体风险概览\n\n")
            f.write(f"![总体风险概览]({summary_chart_path})\n\n")
        
        f.write("### 📊 分组风险评分\n\n")
        for group_name, data in group_scores.items():
            f.write(f"- **{group_name}**: {data['score']:.1f}/100 (权重: {data['weight']}%, 指标数: {data['count']})\n")
        f.write("\n")
        
        # 确定总体风险等级
        if total_score >= 80:
            risk_level = "🔴 高风险"
        elif total_score >= 60:
            risk_level = "🟡 中风险"
        elif total_score >= 40:
            risk_level = "🟢 低风险"
        else:
            risk_level = "🔵 极低风险"
        
        f.write(f"**总体风险等级**: {risk_level}\n\n")
        
        # 指标配置表
        f.write("## 📋 指标配置表\n\n")
        f.write("| 指标名称 | 分组 | 基准分位 | 基准理由 | 变换方法 | 权重 |\n")
        f.write("|---------|------|----------|----------|----------|------|\n")
        
        config_table = [
            ("收益率曲线倒挂: 10年期-3个月", "收益率曲线", "noncrisis_p25", "非危机期25%分位数作为警戒线", "level", "7.5%"),
            ("收益率曲线倒挂: 10年期-2年期", "收益率曲线", "noncrisis_p25", "非危机期25%分位数作为警戒线", "level", "7.5%"),
            ("联邦基金利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("3个月国债利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("10年期国债利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("30年期抵押贷款利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("SOFR隔夜利率", "利率水平", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "3.0%"),
            ("高收益债风险溢价", "信用利差", "crisis_median", "危机期中位数作为警戒线", "level", "7.5%"),
            ("投资级信用利差: Baa-10Y国债", "信用利差", "crisis_median", "危机期中位数作为警戒线", "level", "7.5%"),
            ("芝加哥金融状况指数", "金融状况/波动", "noncrisis_p75", "非危机期75%分位数作为警戒线", "level", "5.0%"),
            ("VIX波动率指数", "金融状况/波动", "noncrisis_p90", "非危机期90%分位数作为警戒线", "level", "5.0%"),
            ("非农就业人数 YoY", "实体经济", "crisis_p25", "危机期25%分位数作为警戒线", "yoy_pct", "5.0%"),
            ("工业生产 YoY", "实体经济", "crisis_p25", "危机期25%分位数作为警戒线", "yoy_pct", "5.0%"),
            ("GDP YoY", "实体经济", "crisis_p25", "危机期25%分位数作为警戒线", "yoy_pct", "5.0%"),
            ("新屋开工（年化）", "房地产", "crisis_p25", "危机期25%分位数作为警戒线", "level", "3.0%"),
            ("房价指数: Case-Shiller 20城 YoY", "房地产", "noncrisis_p90", "非危机期90%分位数作为警戒线", "yoy_pct", "3.0%"),
            ("密歇根消费者信心", "消费", "noncrisis_p35", "非危机期35%分位数作为警戒线", "level", "4.0%"),
            ("消费者信贷 YoY", "消费", "noncrisis_p75", "非危机期75%分位数作为警戒线", "yoy_pct", "4.0%"),
            ("总贷款与租赁 YoY", "银行业", "noncrisis_p75", "非危机期75%分位数作为警戒线", "yoy_pct", "3.5%"),
            ("美联储总资产 YoY", "银行业", "crisis_median", "危机期中位数作为警戒线", "yoy_pct", "3.5%"),
            ("贸易加权美元指数 YoY", "外部环境", "noncrisis_median", "非危机期中位数作为警戒线", "yoy_pct", "5.0%"),
            ("企业债/GDP（名义，%）", "杠杆", "noncrisis_p65", "非危机期65%分位数作为警戒线", "level", "10.0%"),
            ("银行准备金/存款（%）", "银行业", "noncrisis_p25", "非危机期25%分位数作为警戒线", "level", "0.0%"),
            ("银行准备金/总资产（%）", "银行业", "noncrisis_p25", "非危机期25%分位数作为警戒线", "level", "0.0%"),
            ("家庭债务偿付比率", "消费", "crisis_median", "危机期中位数作为警戒线", "level", "0.0%"),
            ("房贷违约率", "房地产", "crisis_median", "危机期中位数作为警戒线", "level", "0.0%")
        ]
        
        for row in config_table:
            f.write(f"| {row[0]} | {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]} |\n")
        
        f.write("\n")
        
        # 基准分位解释
        f.write("## 📊 基准分位解释\n\n")
        f.write("- **crisis_median**: 危机期中位数\n")
        f.write("- **crisis_p25**: 危机期25%分位数\n")
        f.write("- **crisis_p75**: 危机期75%分位数\n")
        f.write("- **noncrisis_median**: 非危机期中位数\n")
        f.write("- **noncrisis_p25**: 非危机期25%分位数\n")
        f.write("- **noncrisis_p35**: 非危机期35%分位数\n")
        f.write("- **noncrisis_p65**: 非危机期65%分位数\n")
        f.write("- **noncrisis_p75**: 非危机期75%分位数\n")
        f.write("- **noncrisis_p90**: 非危机期90%分位数\n\n")
        
        # 历史危机期间参考
        f.write("## 📅 历史危机期间参考\n\n")
        f.write("本报告使用的历史危机期间包括：\n\n")
        for crisis in crisis_periods:
            f.write(f"- **{crisis['name']}** ({crisis['code']}): {crisis['start']} 至 {crisis['end']}\n")
        f.write("\n")
        
        # 免责声明
        f.write("## ⚠️ 免责声明\n\n")
        f.write("本报告仅供参考，不构成投资建议。历史数据不保证未来表现。本报告所有数据下载都存在错误可能，计算存在错误可能。请不要作为投资判断的依据。\n\n")
        f.write("如果你发现任何错误，欢迎指出，发邮件给：jiangx@gmail.com\n\n")
        
        f.write("---\n\n")
        f.write("*本报告基于FRED数据，仅供参考，不构成投资建议*\n")
    
    # 生成HTML报告（使用Base64嵌入）
    html_path = output_dir / f"crisis_report_{timestamp}.html"
    with open(md_path, "r", encoding="utf-8") as f:
        markdown_content = f.read()
    
    # v2.4: 生成双轨风险面板的HTML版本（增加 Structural Floor）
    score_state = tension_metrics.get('score_state', 0.0)
    score_trigger = tension_metrics.get('score_trigger', 0.0)
    trigger_momentum = tension_metrics.get('trigger_momentum', 0.0)
    convexity_factor = tension_metrics.get('convexity_factor', 1.0)
    index_tension = tension_metrics.get('index_tension', 0.0)
    structural_floor = tension_metrics.get('structural_floor', 0.0)
    final_risk_score = tension_metrics.get('final_risk_score', index_tension)
    tension_analysis = tension_metrics.get('tension_analysis', '数据不足')
    
    if trigger_momentum > 0:
        momentum_text = f"+{trigger_momentum:.1f} (恶化 ⚠️)"
        momentum_color = "#c62828"
    else:
        momentum_text = f"{trigger_momentum:.1f} (改善)"
        momentum_color = "#2e7d32"
    
    # v2.4: 判断结构性底座是否激活
    floor_active = (final_risk_score == structural_floor and structural_floor > 0)
    floor_status_text = ""
    if structural_floor > 0:
        if floor_active:
            floor_status_text = f'<div style="background: #ffebee; padding: 8px; border-radius: 4px; margin-top: 10px; border-left: 3px solid #c62828;"><strong>🛡️ 结构性底座已激活 (Structural Floor Active)</strong><br><span style="font-size: 0.9em;">原因：State ({score_state:.1f}) 过高，系统强制锁定最低风险分 {structural_floor:.1f}。</span></div>'
        else:
            floor_status_text = f'<div style="background: #f5f5f5; padding: 8px; border-radius: 4px; margin-top: 10px; font-size: 0.9em; color: #666;">🛡️ 结构性底座: {structural_floor:.1f} (未激活)</div>'
    
    tension_html = f"""
<div style="margin: 20px 0;">
    <div style="display: flex; gap: 20px; margin-bottom: 15px;">
        <div style="flex: 1; background: #e3f2fd; padding: 15px; border-radius: 8px; border-left: 5px solid #2196f3;">
            <h3 style="margin-top: 0; color: #1976d2;">📉 回报不对称性 (Valuation Risk)</h3>
            <p style="font-size: 1.2em; font-weight: bold; color: #1976d2;">得分: {score_state:.1f} / 100</p>
            <p style="color: #555;">含义：资产是否昂贵？未来长期回报是否受限？</p>
            <p style="color: #555; font-size: 0.9em;">策略：分数越高，越应降低预期收益，而非立刻卖出。</p>
        </div>
        <div style="flex: 1; background: #ffebee; padding: 15px; border-radius: 8px; border-left: 5px solid #f44336;">
            <h3 style="margin-top: 0; color: #c62828;">💥 崩盘触发概率 (Trigger Risk)</h3>
            <p style="font-size: 1.2em; font-weight: bold; color: #c62828;">得分: {score_trigger:.1f} / 100</p>
            <p style="color: #555;">含义：流动性/信用/情绪是否在恶化？</p>
            <p style="color: {momentum_color}; font-weight: bold;">🌊 动量: {momentum_text}</p>
            <p style="color: #555; font-size: 0.9em;">策略：分数越高，越应增加对冲或现金。</p>
        </div>
    </div>
    <div style="background: #fff3e0; padding: 15px; border-left: 5px solid #ff9800; border-radius: 4px;">
        <h3 style="margin-top: 0; color: #e65100;">⚡ 系统张力指数 (System Tension): {final_risk_score:.1f} / 100</h3>
        <div style="display: flex; gap: 20px; margin-bottom: 10px; font-size: 0.9em; color: #666;">
            <div>🔥 凸性放大器: <strong>{convexity_factor:.2f}x</strong></div>
            <div>🌊 Trigger动量: <strong style="color: {momentum_color};">{momentum_text}</strong></div>
        </div>
        {floor_status_text}
        <p style="font-weight: bold; color: #e65100; font-size: 1.1em; margin-top: 10px;">{tension_analysis}</p>
        <p style="font-size: 0.85em; color: #888; margin-top: 5px;">
            * v2.4 算法：当 State > 40 时，系统进入"易碎模式" (Fragile Mode)，Trigger 的微小恶化会被凸性放大 {convexity_factor:.1f} 倍。同时，高估值会触发结构性底座，防止风险低估。
        </p>
    </div>
</div>
"""
    
    # 在HTML中替换Markdown版本的市场体制、美股和黄金诊断为HTML版本
    try:
        market_md, market_html = generate_market_regime_diagnosis(processed_indicators)
    except Exception as e:
        print(f"⚠️ 市场体制诊断生成失败: {e}")
        market_md, market_html = "### 🧭 市场体制雷达\n\n*市场数据暂不可用*\n\n", ""
    
    bubble_md, bubble_html = generate_bubble_diagnosis(processed_indicators)
    gold_html, gold_md = generate_gold_diagnosis(processed_indicators)
    
    # 先转换Markdown为HTML
    html_content = render_html_report(markdown_content, "宏观金融危机监察报告", output_dir)
    
    # v2.4: 替换双轨风险面板（Markdown转HTML后会被转义）
    import re
    tension_md_pattern = r'<h2>⚡ v2\.4 双轨风险面板.*?</h2>.*?<hr\s*/>'
    if re.search(tension_md_pattern, html_content, re.DOTALL):
        html_content = re.sub(tension_md_pattern, tension_html, html_content, flags=re.DOTALL)
    else:
        # 如果找不到，在报告开头插入
        body_start = html_content.find('<body>')
        if body_start != -1:
            body_content_start = html_content.find('>', body_start) + 1
            html_content = html_content[:body_content_start] + tension_html + html_content[body_content_start:]
    
    # 替换市场体制诊断（Markdown转HTML后会被转义）
    if market_html:
        market_md_pattern = r'<h3>🧭 市场体制雷达</h3>.*?---'
        if re.search(market_md_pattern, html_content, re.DOTALL):
            html_content = re.sub(market_md_pattern, market_html, html_content, flags=re.DOTALL)
    
    # 替换美股诊断（Markdown转HTML后会被转义）
    bubble_md_pattern = r'<h3>💹 美股泡沫深度诊断</h3>.*?---'
    if re.search(bubble_md_pattern, html_content, re.DOTALL):
        html_content = re.sub(bubble_md_pattern, bubble_html, html_content, flags=re.DOTALL)
    
    # 替换黄金诊断（Markdown转HTML后会被转义）
    gold_md_pattern = r'<h3>🏆 黄金见顶逻辑诊断</h3>.*?属于"晚期狂热"。'
    if re.search(gold_md_pattern, html_content, re.DOTALL):
        html_content = re.sub(gold_md_pattern, gold_html, html_content, flags=re.DOTALL)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    # 生成"最新"副本和latest软链接
    latest_md = output_dir / f"latest_{timestamp}.md"
    latest_html = output_dir / f"latest_{timestamp}.html"
    latest_md.write_text(markdown_content, encoding="utf-8")
    latest_html.write_text(html_content, encoding="utf-8")
    
    # 创建latest软链接（Windows下使用复制）
    latest_md_link = output_dir / "crisis_report_latest.md"
    latest_html_link = output_dir / "crisis_report_latest.html"
    latest_json_link = output_dir / "crisis_report_latest.json"
    
    # 保存JSON数据
    json_path = output_dir / f"crisis_report_{timestamp}.json"
    json_data = {
        "timestamp": timestamp,
        "total_score": total_score,
        "risk_level": risk_level,
        "indicators": processed_indicators,
        "group_scores": group_scores,
        "summary": {
            "high_risk_count": len([i for i in processed_indicators if i['risk_score'] >= 80]),
            "medium_risk_count": len([i for i in processed_indicators if 60 <= i['risk_score'] < 80]),
            "low_risk_count": len([i for i in processed_indicators if i['risk_score'] < 60])
        }
    }
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    try:
        # Windows下使用复制而不是软链接
        import shutil
        if latest_md_link.exists():
            latest_md_link.unlink()
        if latest_html_link.exists():
            latest_html_link.unlink()
        if latest_json_link.exists():
            latest_json_link.unlink()
            
        shutil.copyfile(md_path, latest_md_link)
        shutil.copyfile(html_path, latest_html_link)
        shutil.copyfile(json_path, latest_json_link)
        print(f"✅ 创建latest文件: {latest_md_link.name}, {latest_html_link.name}, {latest_json_link.name}")
    except Exception as e:
        print(f"⚠️ 创建latest文件失败: {e}")
    
    # 生成长图（保存到和HTML同一个目录）
    long_image_path = output_dir / f"crisis_report_long_{timestamp}.png"
    image_success = generate_long_image(str(html_path), str(long_image_path))
    
    print(f"✅ 报告生成完成!")
    print(f"📄 Markdown: {md_path}")
    print(f"🌐 HTML: {html_path}")
    print(f"📊 JSON: {json_path}")
    if image_success:
        print(f"🖼️ 长图: {long_image_path}")
    print(f"📊 图表目录: {output_dir / 'figures'}")
    print(f"🎯 总体风险评分: {total_score:.1f}/100")
    print(f"📊 风险等级: {risk_level}")

if __name__ == "__main__":
    generate_report_with_images()
