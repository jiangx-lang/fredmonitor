#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FRED 危机预警监控系统（优化版）
基于实际对话内容实现的功能：
- FRED数据获取和清洗
- 危机指标计算和风险评估
- Markdown报告生成
- HTML自包含报告（base64图片嵌入）
- 图片生成（中文字体支持）
- 移动端优化
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
from datetime import datetime
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
from dotenv import load_dotenv

# 抑制警告
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

# 工程路径
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

# 配置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 尝试设置中文字体
try:
    fonts = [f.name for f in fm.fontManager.ttflist if 'SimHei' in f.name or 'Microsoft YaHei' in f.name or 'Arial Unicode MS' in f.name]
    if fonts:
        plt.rcParams['font.sans-serif'] = fonts + ['DejaVu Sans']
        print(f"✅ 找到中文字体: {fonts[0]}")
    else:
        print("⚠️ 未找到中文字体，使用默认字体")
except Exception as e:
    print(f"⚠️ 字体配置失败: {e}")

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
                except Exception:
                    continue
    if not loaded:
        print("⚠️ 加载 .env 文件失败，将使用环境变量")
except Exception as e:
    print(f"⚠️ 加载 .env 文件失败: {e}，将使用环境变量")

# FRED API配置
FRED_API_KEY = os.getenv("FRED_API_KEY")
if not FRED_API_KEY:
    print("❌ 未找到FRED_API_KEY环境变量")
    sys.exit(1)

# 动态频率选择（pandas兼容性）
try:
    FREQ_ME = 'ME'  # pandas >= 2.0
    pd.date_range('2020-01-01', periods=1, freq=FREQ_ME)
except ValueError:
    FREQ_ME = 'M'   # pandas < 2.0

def _month_end_code():
    """动态选择月末频率代码"""
    try:
        pd.date_range('2020-01-01', periods=1, freq='ME')
        return 'ME'
    except ValueError:
        return 'M'

def _as_float_series(series: pd.Series) -> pd.Series:
    """将序列转换为float，处理inf和NaN"""
    if series is None or series.empty:
        return pd.Series(dtype=float)
    
    # 转换为数值
    numeric_series = pd.to_numeric(series, errors='coerce')
    
    # 替换inf
    numeric_series = numeric_series.replace([np.inf, -np.inf], np.nan)
    
    return numeric_series

def to_monthly(series: pd.Series, method: str = 'last') -> pd.Series:
    """转换为月频数据"""
    if series is None or series.empty:
        return pd.Series(dtype=float)
    
    # 确保索引是datetime
    series.index = pd.to_datetime(series.index)
    
    # 转换为数值
    series = _as_float_series(series)
    
    # 重采样到月末
    freq_code = _month_end_code()
    monthly = series.resample(freq_code).agg(method)
    
    # 对齐到月末
    monthly = monthly.asfreq(freq_code)
    
    return monthly.dropna()

def transform_series(series: pd.Series, transform: str) -> pd.Series:
    """数据变换"""
    if series is None or series.empty:
        return pd.Series(dtype=float)
    
    series = _as_float_series(series)
    
    if transform == 'level':
        return series
    elif transform == 'pct_change':
        return series.pct_change() * 100
    elif transform == 'yoy_pct':
        return series.pct_change(periods=12) * 100
    elif transform == 'diff':
        return series.diff()
    else:
        return series

def load_yaml_config(file_path: pathlib.Path) -> dict:
    """加载YAML配置文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 加载配置文件失败 {file_path}: {e}")
        return {}

def fetch_series_from_api(series_id: str, start_date: str = "2000-01-01") -> pd.Series:
    """从FRED API获取数据"""
    import requests
    
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = {
        'series_id': series_id,
        'api_key': FRED_API_KEY,
        'file_type': 'json',
        'observation_start': start_date,
        'frequency': 'm'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'observations' not in data:
            return pd.Series(dtype=float)
        
        observations = data['observations']
        dates = []
        values = []
        
        for obs in observations:
            if obs['value'] != '.':
                dates.append(pd.to_datetime(obs['date']))
                values.append(float(obs['value']))
        
        return pd.Series(values, index=dates)
    
    except Exception as e:
        print(f"❌ API获取失败 {series_id}: {e}")
        return pd.Series(dtype=float)

def load_local_series_data(series_id: str) -> pd.Series:
    """加载本地数据"""
    # 尝试新目录结构
    new_path = BASE / "data" / "fred" / "series" / series_id / "raw.csv"
    if new_path.exists():
        try:
            df = pd.read_csv(new_path, index_col=0, parse_dates=True)
            return df.iloc[:, 0] if len(df.columns) > 0 else pd.Series(dtype=float)
        except Exception:
            pass
    
    # 尝试旧目录结构
    old_paths = list(BASE.glob(f"data/fred/categories/*/series/{series_id}/raw.csv"))
    if old_paths:
        try:
            df = pd.read_csv(old_paths[0], index_col=0, parse_dates=True)
            return df.iloc[:, 0] if len(df.columns) > 0 else pd.Series(dtype=float)
        except Exception:
            pass
    
    return pd.Series(dtype=float)

def get_series_data(series_id: str, start_date: str = "2000-01-01") -> pd.Series:
    """获取序列数据（本地优先，API兜底）"""
    # 先尝试本地数据
    local_data = load_local_series_data(series_id)
    if not local_data.empty:
        return local_data
    
    # 本地无数据，尝试API
    print(f"⚠️ 本地无数据/坏数据，尝试API获取 {series_id}")
    api_data = fetch_series_from_api(series_id, start_date)
    
    if not api_data.empty:
        return api_data
    
    return pd.Series(dtype=float)

def calculate_crisis_stats(series: pd.Series, crises: List[dict]) -> Dict[str, float]:
    """计算危机期间统计量"""
    if series is None or series.empty:
        return {}
    
    crisis_values = []
    for crisis in crises:
        start_date = pd.to_datetime(crisis['start'])
        end_date = pd.to_datetime(crisis['end'])
        
        # 对齐到月末
        start_date = start_date.to_period('M').to_timestamp('M')
        end_date = end_date.to_period('M').to_timestamp('M')
        
        crisis_data = series[(series.index >= start_date) & (series.index <= end_date)]
        if len(crisis_data) >= 6:  # 至少6个数据点
            crisis_values.extend(crisis_data.dropna().values)
    
    if len(crisis_values) < 6:
        # 危机数据不足，使用全样本
        crisis_values = series.dropna().values
    
    if len(crisis_values) == 0:
        return {}
    
    crisis_values = np.array(crisis_values)
    crisis_values = crisis_values[~np.isnan(crisis_values)]
    
    if len(crisis_values) == 0:
        return {}
    
    return {
        'crisis_median': float(np.nanmedian(crisis_values)),
        'crisis_p25': float(np.nanpercentile(crisis_values, 25)),
        'crisis_p75': float(np.nanpercentile(crisis_values, 75)),
        'crisis_mean': float(np.nanmean(crisis_values)),
        'crisis_std': float(np.nanstd(crisis_values))
    }

def calculate_risk_score(current_value: float, crisis_stats: Dict[str, float], 
                         higher_is_risk: bool = True, compare_to: str = 'crisis_median') -> float:
    """计算风险评分"""
    if not crisis_stats or compare_to not in crisis_stats:
        return 0.0
    
    benchmark = crisis_stats[compare_to]
    
    if higher_is_risk:
        # 高值高风险：当前值越高，风险越大
        if current_value >= benchmark:
            deviation = (current_value - benchmark) / abs(benchmark) * 100
        else:
            deviation = -(benchmark - current_value) / abs(benchmark) * 100
    else:
        # 低值高风险：当前值越低，风险越大
        if current_value <= benchmark:
            deviation = (benchmark - current_value) / abs(benchmark) * 100
        else:
            deviation = -(current_value - benchmark) / abs(benchmark) * 100
    
    # 风险评分：0-100
    risk_score = max(0, min(100, 50 + deviation))
    return round(risk_score, 1)

def save_indicator_plot(ts: pd.Series, title: str, unit: str, crises: List[dict], 
                       crisis_stats: Dict[str, float], out_path: pathlib.Path,
                       show_ma: Optional[List[int]] = (6, 12), annotate_latest: bool = True):
    """保存指标图表"""
    if ts is None or ts.dropna().empty:
        return
    
    ts = ts.dropna().copy()
    ts.index = pd.to_datetime(ts.index)
    
    # 计算参考线
    long_mean = float(np.nanmean(ts.values)) if len(ts) else np.nan
    crisis_median = crisis_stats.get('crisis_median', np.nan)
    
    # 绘图
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig = plt.figure(figsize=(12, 6), dpi=150)
    ax = plt.gca()
    
    # 确保中文字体生效
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    
    # 绘制主序列
    ax.plot(ts.index, ts.values, 'b-', linewidth=1.5, alpha=0.8, label='原始数据')
    
    # 绘制移动平均线
    if show_ma:
        for period in show_ma:
            ma = ts.rolling(window=period, min_periods=1).mean()
            ax.plot(ma.index, ma.values, '--', alpha=0.7, linewidth=1, 
                   label=f'{period}月均线')
    
    # 绘制参考线
    if not np.isnan(long_mean):
        ax.axhline(y=long_mean, color='gray', linestyle=':', alpha=0.7, 
                  label=f'历史均值: {long_mean:.2f}')
    
    if not np.isnan(crisis_median):
        ax.axhline(y=crisis_median, color='red', linestyle='--', alpha=0.7, 
                  label=f'危机中位数: {crisis_median:.2f}')
    
    # 绘制危机期间
    for crisis in crises:
        start_date = pd.to_datetime(crisis['start'])
        end_date = pd.to_datetime(crisis['end'])
        ax.axvspan(start_date, end_date, alpha=0.2, color='red', 
                  label=crisis.get('name', '危机期间'))
    
    # 标注最新值
    if annotate_latest and len(ts) > 0:
        latest_date = ts.index[-1]
        latest_value = ts.iloc[-1]
        ax.scatter([latest_date], [latest_value], color='red', s=50, zorder=5)
        ax.annotate(f'当前: {latest_value:.2f}', 
                   xy=(latest_date, latest_value),
                   xytext=(10, 10), textcoords='offset points',
                   bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.7),
                   arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0'))
    
    # 设置标题和标签
    ax.set_title(f'{title} ({unit})', fontsize=14, fontweight='bold')
    ax.set_xlabel('时间', fontsize=12)
    ax.set_ylabel(unit, fontsize=12)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # 格式化x轴
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches='tight')
    plt.close(fig)

def _read_png_as_base64(p: pathlib.Path) -> str | None:
    """读取PNG文件并转换为base64字符串"""
    try:
        with open(p, "rb") as f:
            return base64.b64encode(f.read()).decode("ascii")
    except Exception:
        return None

_IMG_MD_RE = re.compile(r"!\[[^\]]*\]\((?P<path>[^)]+\.png)\)")

def render_html_report(md_text: str, report_title: str, report_dir: pathlib.Path) -> str:
    """把 Markdown 中的相对 PNG 链接替换为 base64 <img>，输出完整 HTML 字符串"""
    def repl(m: re.Match) -> str:
        rel = m.group("path")
        img_path = (report_dir / rel).resolve()
        b64 = _read_png_as_base64(img_path)
        if not b64:
            # 读不到就保留原始相对链接（降级）
            return f'<img alt="" src="{rel}"/>'
        return f'<img alt="" src="data:image/png;base64,{b64}"/>'

    body = _IMG_MD_RE.sub(repl, md_text)

    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{report_title}</title>
<style>
  body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;line-height:1.6;padding:16px;max-width:940px;margin:auto;}}
  img{{max-width:100%;height:auto;display:block;margin:8px 0;}}
  code,pre{{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;}}
  h1,h2,h3{{line-height:1.25}}
  .kpi{{display:flex;gap:12px;flex-wrap:wrap}}
  .kpi div{{padding:8px 12px;border-radius:10px;background:#f5f5f7}}
</style>
</head>
<body>
<article>
{body}
</article>
</body>
</html>"""
    return html

def generate_markdown_report(results: List[dict], crises: List[dict], output_file: pathlib.Path):
    """生成Markdown报告"""
    lines = []
    
    # 标题
    lines.append("# 🚨 FRED 宏观金融危机预警监控报告")
    lines.append("")
    lines.append(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
    lines.append("")
    
    # 执行摘要
    lines.append("## 📊 执行摘要")
    lines.append("")
    
    # 统计信息
    total_indicators = len(results)
    high_risk = len([r for r in results if r.get('risk_score', 0) >= 70])
    medium_risk = len([r for r in results if 40 <= r.get('risk_score', 0) < 70])
    low_risk = len([r for r in results if r.get('risk_score', 0) < 40])
    
    lines.append(f"- **总指标数**: {total_indicators}")
    lines.append(f"- **高风险指标**: {high_risk} 个")
    lines.append(f"- **中风险指标**: {medium_risk} 个")
    lines.append(f"- **低风险指标**: {low_risk} 个")
    lines.append("")
    
    # 风险等级说明
    lines.append("### 🎯 风险等级说明")
    lines.append("- 🔴 **高风险** (70-100分): 当前值显著偏离历史危机水平")
    lines.append("- 🟡 **中风险** (40-69分): 当前值略高于历史危机水平")
    lines.append("- 🟢 **低风险** (0-39分): 当前值接近或低于历史危机水平")
    lines.append("")
    
    # 详细指标
    lines.append("## 📈 详细指标分析")
    lines.append("")
    
    for i, result in enumerate(results, 1):
        series_id = result.get('series_id', '')
        name = result.get('name', '')
        current_value = result.get('current_value', 0)
        unit = result.get('unit', '')
        risk_score = result.get('risk_score', 0)
        benchmark_value = result.get('benchmark_value', 0)
        benchmark_type = result.get('benchmark_type', '')
        explanation = result.get('explanation', '')
        figure = result.get('figure', '')
        
        # 风险等级
        if risk_score >= 70:
            risk_level = "🔴 高风险"
        elif risk_score >= 40:
            risk_level = "🟡 中风险"
        else:
            risk_level = "🟢 低风险"
        
        lines.append(f"### {i}. {name} ({series_id})")
        lines.append("")
        lines.append(f"- **当前值**: {current_value:.4f} {unit}")
        lines.append(f"- **基准值**: {benchmark_value:.4f} ({benchmark_type})")
        lines.append(f"- **风险评分**: {risk_score} ({risk_level})")
        lines.append("")
        lines.append(f"- **解释**: {explanation}")
        lines.append("")
        
        # 添加图片
        if figure:
            lines.append(f"![{series_id}]({figure})")
            lines.append("")
    
    # 免责声明
    lines.append("")
    lines.append("## ⚠️ 免责声明")
    lines.append("本报告仅供参考，不构成投资建议。历史数据不保证未来表现。")

    output_file.write_text("\n".join(lines), encoding="utf-8")

def generate_outputs(results: List[dict], crises: List[dict], timestamp: str):
    """生成所有输出文件"""
    out_dir = BASE / "outputs" / "crisis_monitor"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = timestamp

    # 1) 写 CSV
    csv_path = out_dir / f"crisis_report_{ts}.csv"
    pd.DataFrame(results).to_csv(csv_path, index=False, encoding="utf-8-sig")

    # 2) 写 Markdown
    md_path = out_dir / f"crisis_report_{ts}.md"
    generate_markdown_report(results, crises, md_path)

    # 3) 生成 HTML（把 md 读进来，再转成自包含 HTML）
    md_text = md_path.read_text(encoding="utf-8")
    html_text = render_html_report(md_text, report_title="宏观金融危机监察报告", report_dir=out_dir)
    html_path = out_dir / "report.html"  # 本次 run 的自包含 HTML
    html_path.write_text(html_text, encoding="utf-8")

    # 4) 生成"最新"副本（手机上只点 latest.html 就行）
    latest_md = out_dir / "latest.md"
    latest_html = out_dir / "latest.html"
    latest_md.write_text(md_text, encoding="utf-8")
    latest_html.write_text(html_text, encoding="utf-8")

    # 5) JSON（保留原有功能）
    json_path = out_dir / f"crisis_report_{ts}.json"
    json_path.write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    print("\n📄 报告已生成：")
    print(f"  📊 CSV: {csv_path}")
    print(f"  📝 MD : {md_path}")
    print(f"  🌐 HTML(自包含): {html_path}")
    print(f"  👉 快捷打开: {latest_html}")
    print(f"  📋 JSON: {json_path}")

def generate_crisis_report():
    """生成危机预警报告"""
    print("🚨 FRED 危机预警监控系统启动...")
    print("=" * 80)

    # 加载配置
    indicators_cfg = load_yaml_config(BASE / "config" / "crisis_indicators.yaml")
    crises_cfg = load_yaml_config(BASE / "config" / "crisis_periods.yaml")
    
    if not indicators_cfg or not crises_cfg:
        print("❌ 配置文件加载失败")
        return []
    
    indicators = indicators_cfg.get("indicators", [])
    crises = crises_cfg.get("crises", [])

    print(f"📊 指标数: {len(indicators)}")
    print(f"📅 危机段: {len(crises)}")

    results: List[dict] = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 处理每个指标
    for i, indicator in enumerate(indicators, 1):
        series_id = indicator.get('series_id', '')
        name = indicator.get('name', '')
        transform = indicator.get('transform', 'level')
        higher_is_risk = indicator.get('higher_is_risk', True)
        compare_to = indicator.get('compare_to', 'crisis_median')
        unit_hint = indicator.get('unit_hint', '')
        plain_explainer = indicator.get('plain_explainer', '')
        
        print(f"\n[{i}/{len(indicators)}] 处理 {name} ({series_id})...")
        
        try:
            # 获取数据
            raw_data = get_series_data(series_id)
            if raw_data.empty:
                print(f"  ❌ 无数据")
                continue
            
            # 转换为月频
            monthly_data = to_monthly(raw_data)
            if monthly_data.empty:
                print(f"  ❌ 月频转换失败")
                continue
            
            # 数据变换
            transformed_data = transform_series(monthly_data, transform)
            if transformed_data.empty:
                print(f"  ❌ 数据变换失败")
                continue
            
            # 计算危机统计
            crisis_stats = calculate_crisis_stats(transformed_data, crises)
            if not crisis_stats:
                print(f"  ❌ 危机统计计算失败")
                continue
            
            # 获取当前值
            current_value = float(transformed_data.iloc[-1]) if len(transformed_data) > 0 else 0.0
            
            # 计算风险评分
            risk_score = calculate_risk_score(current_value, crisis_stats, higher_is_risk, compare_to)
            
            # 生成图表
            figure_rel = None
            try:
                fig_path = BASE / "outputs" / "crisis_monitor" / "figures" / f"{series_id}_latest.png"
                save_indicator_plot(
                    transformed_data, 
                    name, 
                    unit_hint, 
                    crises, 
                    crisis_stats, 
                    fig_path
                )
                
                # 计算相对路径
                md_file = BASE / "outputs" / "crisis_monitor" / f"crisis_report_{timestamp}.md"
                rel_path = fig_path.relative_to(md_file.parent)
                figure_rel = str(rel_path).replace('\\', '/')
                
            except Exception as e:
                print(f"  ⚠️ 图表生成失败: {e}")
                figure_rel = None
            
            # 保存结果
            result = {
                'series_id': series_id,
                'name': name,
                'current_value': current_value,
                'unit': unit_hint,
                'risk_score': risk_score,
                'benchmark_value': crisis_stats.get(compare_to, 0),
                'benchmark_type': compare_to,
                'explanation': plain_explainer,
                'figure': figure_rel
            }
            results.append(result)
            
            print(f"  ✅ 当前值: {current_value:.4f} {unit_hint}")
            print(f"  📊 基准值: {crisis_stats.get(compare_to, 0):.4f} ({compare_to})")
            print(f"  📈 风险评分: {risk_score} ({'🔴 高风险' if risk_score >= 70 else '🟡 中风险' if risk_score >= 40 else '🟢 低风险'})")
            
        except Exception as e:
            print(f"  ❌ 处理失败: {e}")
            continue

    # 生成输出文件
    if results:
        generate_outputs(results, crises, timestamp)
        
        print(f"\n🎉 危机预警报告生成完成！")
        print(f"   ✅ 成功处理: {len(results)} 个指标")
        print(f"   📊 总计指标: {len(indicators)} 个")
    
    return results

if __name__ == "__main__":
    generate_crisis_report()

