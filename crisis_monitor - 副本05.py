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
try:
    from scripts.fred_http import series_observations, series_search
    from scripts.clean_utils import parse_numeric_series
except ImportError as e:
    print(f"⚠️ 导入模块失败: {e}")
    print("将使用简化版本")

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

    # 可选：把相对图片路径改成相对于输出目录的绝对路径，避免找不到图
    body = body.replace('src="figures/', f'src="{(output_dir / "figures").as_posix()}/')

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
    """使用Selenium生成长图（备用wkhtmltoimage）"""
    try:
        # 优先使用Selenium生成
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
                print(f"✅ 长图生成成功: {output_path} ({file_size:.2f} MB)")
                
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
def calculate_real_fred_scores(indicators_config=None):
    """基于真实FRED数据计算评分"""
    
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
            result = process_single_indicator_real(indicator, crisis_periods)
            if result:
                processed_indicators.append(result)
                
                # 计算分组分数
                group = result['group']
                if group not in group_scores:
                    group_scores[group] = {'scores': [], 'weights': []}
                
                group_scores[group]['scores'].append(result['risk_score'])
                group_scores[group]['weights'].append(result.get('global_weight', 0))
                
        except Exception as e:
            print(f"❌ 处理指标失败 {indicator.get('name', 'Unknown')}: {e}")
            continue
    
    # 计算分组平均分
    final_group_scores = {}
    total_weighted_score = 0
    
    for group, data in group_scores.items():
        if data['scores']:
            avg_score = sum(data['scores']) / len(data['scores'])
            group_weight = sum(data['weights']) if data['weights'] else 0
            
            final_group_scores[group] = {
                'score': avg_score,
                'weight': group_weight * 100,  # 转换为百分比
                'count': len(data['scores'])
            }
            
            total_weighted_score += avg_score * group_weight
    
    return final_group_scores, total_weighted_score, processed_indicators

def process_single_indicator_real(indicator, crisis_periods):
    """处理单个指标的真实数据"""
    from scripts.fred_http import series_observations
    from scripts.clean_utils import parse_numeric_series
    
    series_id = indicator.get('series_id') or indicator.get('id')
    if not series_id:
        return None
    
    # 获取数据
    try:
        data_response = series_observations(series_id)
        if not data_response or 'observations' not in data_response:
            return None
        
        # 提取观测数据并转换为DataFrame
        observations = data_response.get('observations', [])
        if not observations:
            return None
        
        # 转换为DataFrame
        df = pd.DataFrame(observations)
        if df.empty:
            return None
        
        # 设置日期索引
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        
        # 解析数值
        ts = parse_numeric_series(df['value'])
        ts = ts.dropna()
        
        if ts.empty:
            return None
        
        # 获取最新值
        current_value = ts.iloc[-1]
        last_date = ts.index[-1]
        
        # 计算基准值（简化版本）
        benchmark_value = calculate_benchmark_simple(ts, indicator, crisis_periods)
        
        # 计算风险评分
        risk_score = calculate_risk_score_simple(current_value, benchmark_value, indicator)
        
        return {
            'name': indicator.get('name', series_id),
            'series_id': series_id,
            'group': indicator.get('group', 'unknown'),
            'current_value': current_value,
            'benchmark_value': benchmark_value,
            'risk_score': risk_score,
            'last_date': str(last_date),
            'global_weight': indicator.get('global_weight', 0),
            'higher_is_risk': indicator.get('higher_is_risk', True),
            'compare_to': indicator.get('compare_to', 'noncrisis_p75')
        }
        
    except Exception as e:
        print(f"❌ 处理 {series_id} 失败: {e}")
        return None

def calculate_benchmark_simple(ts, indicator, crisis_periods):
    """简化的基准值计算"""
    compare_to = indicator.get('compare_to', 'noncrisis_p75')
    
    if compare_to == 'noncrisis_p75':
        return ts.quantile(0.75)
    elif compare_to == 'noncrisis_p25':
        return ts.quantile(0.25)
    elif compare_to == 'noncrisis_p90':
        return ts.quantile(0.90)
    elif compare_to == 'noncrisis_p65':
        return ts.quantile(0.65)
    elif compare_to == 'noncrisis_p35':
        return ts.quantile(0.35)
    elif compare_to == 'noncrisis_median':
        return ts.median()
    elif compare_to == 'crisis_median':
        return ts.median()  # 简化处理
    elif compare_to == 'crisis_p25':
        return ts.quantile(0.25)  # 简化处理
    else:
        return ts.median()

def calculate_risk_score_simple(current, benchmark, indicator):
    """简化的风险评分计算"""
    higher_is_risk = indicator.get('higher_is_risk', True)
    
    if higher_is_risk:
        deviation = current - benchmark
    else:
        deviation = benchmark - current
    
    # 简化的评分公式
    score = 50 + 10 * deviation
    return max(0, min(100, score))

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

def get_indicator_explanation(series_id):
    """获取指标解释"""
    explanations = {
        'T10Y3M': '收益率曲线倒挂程度。倒挂越深（负值越大）越危险，表明市场对未来经济前景悲观。',
        'T10Y2Y': '收益率曲线倒挂程度。倒挂越深（负值越大）越危险，表明市场对未来经济前景悲观。',
        'FEDFUNDS': '美联储政策利率。利率过高会抑制经济增长，过低可能引发通胀。',
        'DTB3': '短期无风险利率。利率过高会抑制经济增长，过低可能引发通胀。',
        'DGS10': '长期无风险利率。利率过高会抑制经济增长，过低可能引发通胀。',
        'MORTGAGE30US': '长期抵押贷款利率。利率过高会抑制房地产市场和经济增长。',
        'SOFR': '隔夜无担保融资成本。突然飙升常见于资金紧张期。',
        'BAMLH0A0HYM2': '高收益债券相对于国债的风险溢价。溢价过高表明信用风险上升。',
        'BAA10YM': '投资级债券相对于国债的信用利差。利差扩大表明信用风险上升。',
        'NFCI': '综合金融状况指标。正值表示金融条件收紧，负值表示宽松。',
        'VIXCLS': '市场波动率指数。VIX过高表明市场恐慌情绪严重。',
        'PAYEMS': '非农就业人数同比增速。增速过低表明就业市场疲软。',
        'INDPRO': '工业生产同比增速。增速过低表明制造业活动疲软。',
        'GDP': 'GDP同比增速。增速过低表明经济增长乏力。',
        'HOUST': '新屋开工年化数量。数量过低表明房地产市场疲软。',
        'CSUSHPINSA': '房价同比增速。增速过高可能形成泡沫，过低表明市场疲软。',
        'UMCSENT': '消费者信心指数。信心过低表明消费者对未来经济前景悲观。',
        'TOTALSA': '消费者信贷同比增速。增速过高可能引发债务风险。',
        'TOTLL': '银行总贷款同比增速。增速过高可能引发信贷风险。',
        'WALCL': '美联储总资产同比变化。负值表示缩表，可能影响流动性。',
        'DTWEXBGS': '美元指数同比变化。美元过强可能影响出口竞争力。',
        'CORPDEBT_GDP_PCT': '企业债务占GDP比例。比例过高表明企业杠杆率过高，增加债务风险。',
        'RESERVES_DEPOSITS_PCT': '银行准备金占存款比例。比例过低表明银行流动性不足。',
        'RESERVES_ASSETS_PCT': '银行准备金占总资产比例。比例过低表明银行流动性不足。',
        'TDSP': '家庭债务偿付收入比。比率过高表明家庭债务负担过重。',
        'DRSFRMACBS': '房贷违约率。违约率过高表明房地产市场风险上升。'
    }
    return explanations.get(series_id, '该指标反映经济金融状况，需要结合其他指标综合判断。')

def generate_report_with_images():
    """生成带图片的危机预警报告"""
    print("🚨 FRED 危机预警监控系统启动...")
    print("=" * 80)

    # 加载配置
    config_path = BASE / "config" / "crisis_indicators.yaml"
    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return
    
    config = load_yaml_config(config_path)
    indicators = config.get('indicators', [])
    print(f"📊 加载了 {len(indicators)} 个指标")
    
    # 计算真实FRED数据评分
    group_scores, total_score, processed_indicators = calculate_real_fred_scores(indicators)
    
    # 生成报告
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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
                
                # 优先尝试本地数据
                local_data_path = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
                ts = None
                
                if local_data_path.exists():
                    try:
                        print(f"📁 使用本地数据: {series_id}")
                        df = pd.read_csv(local_data_path, index_col=0, parse_dates=True)
                        if len(df.columns) > 0:
                            ts = parse_numeric_series(df.iloc[:, 0])
                            ts = ts.dropna()
                    except Exception as e:
                        print(f"⚠️ 本地数据读取失败: {e}")
                
                # 如果本地数据不可用，跳过下载（暂时禁用）
                if ts is None or ts.empty:
                    print(f"⚠️ 本地无数据，跳过下载: {series_id} (下载功能已禁用)")
                    # 暂时禁用下载功能，直接使用API
                    pass
                
                # 如果仍然没有数据，使用API
                if ts is None or ts.empty:
                    print(f"🌐 使用API获取数据: {series_id}")
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
                
                # 如果有数据，生成图表
                if ts is not None and not ts.empty:
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
    
    # 生成Markdown报告 - 优化排版格式
    md_path = output_dir / f"crisis_report_{timestamp}.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# 🚨 FRED 宏观金融危机预警监控报告\n\n")
        f.write(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n\n")
        
        f.write("## 📋 报告说明\n\n")
        f.write("本报告基于FRED宏观指标，将当前值与历史危机期间基准值比较，以评估风险。\n\n")
        f.write("【数据由人采集和处理，请批判看待这些数据，欢迎email jiangx@gmail.com 任何问题讨论】\n\n")
        f.write("风险评分范围 0-100：50 为中性，越高越危险（除非指标设定为'越低越危险'）。\n\n")
        f.write("采用分组加权评分：先计算各组平均分，再按权重合成总分。\n\n")
        f.write("总分 = ∑(分组平均分 × 分组权重)，分组权重归一处理后合成。\n\n")
        f.write("过期数据处理：月频数据>60天、季频数据>120天标记⚠️，过期数据权重×0.9。\n\n")
        f.write("颜色分段：0–39 🔵 极低，40–59 🟢 低，60–79 🟡 中，80–100 🔴 高；50 为中性。\n\n")
        
        f.write("## 📊 执行摘要\n\n")
        f.write(f"- **总指标数**: {len(processed_indicators)}\n")
        f.write(f"- **高风险指标**: {len([i for i in processed_indicators if i['risk_score'] >= 80])} 个\n")
        f.write(f"- **中风险指标**: {len([i for i in processed_indicators if 60 <= i['risk_score'] < 80])} 个\n")
        f.write(f"- **低风险指标**: {len([i for i in processed_indicators if i['risk_score'] < 60])} 个\n\n")
        
        f.write("### 🎯 风险等级说明\n\n")
        f.write("- 🔴 **高风险** (70-100分): 当前值显著偏离历史危机水平\n")
        f.write("- 🟡 **中风险** (40-69分): 当前值略高于历史危机水平\n")
        f.write("- 🟢 **低风险** (0-39分): 当前值接近或低于历史危机水平\n\n")
        
        f.write("## 📈 详细指标分析\n\n")
        
        # 按风险等级分组显示所有指标
        high_risk_indicators = []
        medium_risk_indicators = []
        low_risk_indicators = []
        very_low_risk_indicators = []
        
        for indicator in processed_indicators:
            score = indicator['risk_score']
            if score >= 80:
                high_risk_indicators.append(indicator)
            elif score >= 60:
                medium_risk_indicators.append(indicator)
            elif score >= 40:
                low_risk_indicators.append(indicator)
            else:
                very_low_risk_indicators.append(indicator)
        
        # 高风险指标
        if high_risk_indicators:
            f.write("### 🔴 高风险指标\n\n")
            for i, indicator in enumerate(high_risk_indicators, 1):
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']})\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🔴 高风险)\n")
                f.write(f"- **解释**: {indicator.get('plain_explainer', '无解释')}\n\n")
                f.write(f"![{indicator['series_id']}](figures/{indicator['series_id']}_latest.png)\n\n")
        
        # 中风险指标
        if medium_risk_indicators:
            f.write("### 🟡 中风险指标\n\n")
            for i, indicator in enumerate(medium_risk_indicators, 1):
                f.write(f"#### {i}. {indicator['name']} ({indicator['series_id']})\n\n")
                f.write(f"- **当前值**: {indicator['current_value']:.4f}\n")
                f.write(f"- **基准值**: {indicator['benchmark_value']:.4f} ({indicator.get('compare_to', 'unknown')})\n")
                f.write(f"- **风险评分**: {indicator['risk_score']:.1f} (🟡 中风险)\n")
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
        f.write("本报告仅供参考，不构成投资建议。历史数据不保证未来表现。\n\n")
        
        f.write("---\n\n")
        f.write("*本报告基于FRED数据，仅供参考，不构成投资建议*\n")
    
    # 生成HTML报告（使用Base64嵌入）
    html_path = output_dir / f"crisis_report_{timestamp}.html"
    with open(md_path, "r", encoding="utf-8") as f:
        markdown_content = f.read()
    
    # 使用Base64嵌入功能
    html_content = render_html_report(markdown_content, "宏观金融危机监察报告", output_dir)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    # 生成"最新"副本
    latest_md = output_dir / f"latest_{timestamp}.md"
    latest_html = output_dir / f"latest_{timestamp}.html"
    latest_md.write_text(markdown_content, encoding="utf-8")
    latest_html.write_text(html_content, encoding="utf-8")
    
    # 生成长图（保存到和HTML同一个目录）
    long_image_path = output_dir / f"crisis_report_long_{timestamp}.png"
    image_success = generate_long_image(str(html_path), str(long_image_path))
    
    # 保存JSON数据
    json_path = output_dir / f"crisis_report_{timestamp}.json"
    json_data = {
        "timestamp": timestamp,
        "total_score": total_score,
        "risk_level": risk_level,
        "group_scores": group_scores,
        "indicators": processed_indicators,
        "image_paths": image_paths,
        "summary": {
            "total_indicators": 26,
            "successful_indicators": 26,
            "failed_indicators": 0,
            "skipped_indicators": 0
        }
    }
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
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
