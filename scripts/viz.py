# scripts/viz.py
# -*- coding: utf-8 -*-
import pathlib
from typing import Dict, List, Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import matplotlib.font_manager as fm

# 配置中文字体 - 更全面的字体支持
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS', 'DejaVu Sans']
matplotlib.rcParams['axes.unicode_minus'] = False
matplotlib.rcParams['font.size'] = 10

# 尝试设置中文字体
try:
    import matplotlib.font_manager as fm
    # 查找系统中可用的中文字体
    fonts = [f.name for f in fm.fontManager.ttflist if 'SimHei' in f.name or 'Microsoft YaHei' in f.name or 'Arial Unicode MS' in f.name]
    if fonts:
        matplotlib.rcParams['font.sans-serif'] = fonts + ['DejaVu Sans']
        print(f"✅ 找到中文字体: {fonts[0]}")
    else:
        print("⚠️ 未找到中文字体，使用默认字体")
except Exception as e:
    print(f"⚠️ 字体配置失败: {e}")

def _ensure_dir(p: pathlib.Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def _to_month_end(dtlike) -> pd.Timestamp:
    return pd.to_datetime(dtlike).to_period("M").to_timestamp("M")

def save_indicator_plot(
    ts: pd.Series,
    title: str,
    unit: str,
    crises: List[dict],
    crisis_stats: Dict[str, float],
    out_path: pathlib.Path,
    show_ma: Optional[List[int]] = (6, 12),   # 6/12月滚动均线
    annotate_latest: bool = True,
):
    """
    ts: 月频（已对齐到月末）的序列（float）
    crises: [{'name','start','end',...}]
    crisis_stats: {'crisis_median','crisis_p25','crisis_p75','crisis_mean', ...}
    """
    if ts is None or ts.dropna().empty:
        return

    ts = ts.dropna().copy()
    ts.index = pd.to_datetime(ts.index)

    # 计算参考线
    long_mean = float(np.nanmean(ts.values)) if len(ts) else np.nan
    crisis_median = float(crisis_stats.get("crisis_median", np.nan))

    # 绘图
    _ensure_dir(out_path)
    fig = plt.figure(figsize=(12, 6), dpi=150)
    ax = plt.gca()
    
    # 确保中文字体生效
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

    # 主序列
    ax.plot(ts.index, ts.values, linewidth=1.3, label="Series")

    # 滚动均线
    if show_ma:
        for w in show_ma:
            if w > 1 and len(ts) >= w:
                ax.plot(ts.index, ts.rolling(w).mean(), linewidth=1.0, linestyle="--", label=f"MA{w}")

    # 参考水平线：全样本均值 & 危机中位数
    if not np.isnan(long_mean):
        ax.axhline(long_mean, linestyle=":", linewidth=1.0, label=f"Hist. mean = {long_mean:.2f}")
    if not np.isnan(crisis_median):
        ax.axhline(crisis_median, linestyle=":", linewidth=1.2, label=f"Crisis median = {crisis_median:.2f}")

    # 危机区间着色
    for c in crises or []:
        try:
            s = _to_month_end(c["start"])
            e = _to_month_end(c["end"])
            ax.axvspan(s, e, alpha=0.15)  # 默认颜色+透明度即可
        except Exception:
            continue

    # 最新值标注
    if annotate_latest and len(ts) > 0:
        x = ts.index[-1]
        y = float(ts.iloc[-1])
        ax.scatter([x], [y], zorder=3)
        ax.annotate(
            f"Latest: {y:.2f} {unit}".strip(),
            xy=(x, y),
            xytext=(10, 10),
            textcoords="offset points",
            fontsize=9,
            bbox=dict(boxstyle="round,pad=0.2", alpha=0.2),
            arrowprops=dict(arrowstyle="->", lw=0.8),
        )

    # 轴/标题/图例
    ax.set_title(title, fontsize=12)
    ax.set_ylabel(unit, fontsize=10)
    ax.xaxis.set_major_formatter(DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best", fontsize=8)

    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)

def create_long_chart(results: List[dict], crises: List[dict], output_path: pathlib.Path):
    """
    创建包含所有指标的长图
    """
    # 过滤成功的指标
    successful_results = [r for r in results if r.get("status") == "success"]
    
    if not successful_results:
        print("❌ 没有成功的指标数据，无法生成长图")
        return
    
    # 计算子图布局
    n_indicators = len(successful_results)
    n_cols = 3  # 每行3个图
    n_rows = math.ceil(n_indicators / n_cols)
    
    # 创建大图
    fig_width = 18  # 增加宽度以适应3列
    fig_height = n_rows * 4  # 每行4英寸高度
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(fig_width, fig_height))
    fig.suptitle('宏观金融危机监察报告 - 所有指标图表', fontsize=16, fontweight='bold')
    
    # 确保axes是二维数组
    if n_rows == 1:
        axes = axes.reshape(1, -1)
    elif n_cols == 1:
        axes = axes.reshape(-1, 1)
    
    # 为每个指标创建子图
    for i, result in enumerate(successful_results):
        row = i // n_cols
        col = i % n_cols
        ax = axes[row, col]
        
        try:
            # 从结果中获取数据
            series_id = result["series_id"]
            indicator_name = result["indicator"]
            current_value = result["current_value"]
            risk_score = result["risk_score"]
            risk_level = result["risk_level"]
            unit = result.get("unit", "")
            
            # 这里我们需要重新获取数据来绘图
            # 由于我们没有原始数据，我们创建一个简化的图表
            ax.text(0.5, 0.5, 
                   f"{indicator_name}\n"
                   f"当前值: {current_value} {unit}\n"
                   f"风险评分: {risk_score:.1f}/100\n"
                   f"风险等级: {risk_level}",
                   ha='center', va='center',
                   fontsize=10,
                   bbox=dict(boxstyle="round,pad=0.5", 
                           facecolor='lightblue' if risk_score < 50 else 'lightcoral',
                           alpha=0.7))
            
            ax.set_title(f"{series_id}", fontsize=9)
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.axis('off')
            
        except Exception as e:
            ax.text(0.5, 0.5, f"图表生成失败\n{str(e)}", 
                   ha='center', va='center', fontsize=8, color='red')
            ax.axis('off')
    
    # 隐藏多余的子图
    for i in range(n_indicators, n_rows * n_cols):
        row = i // n_cols
        col = i % n_cols
        axes[row, col].axis('off')
    
    plt.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"📊 长图已生成: {output_path}")

def create_detailed_long_chart(results: List[dict], crises: List[dict], output_path: pathlib.Path, 
                              data_cache: dict = None):
    """
    创建包含详细图表的长图（需要原始数据）
    """
    if data_cache is None:
        print("❌ 需要数据缓存来生成详细长图")
        return create_long_chart(results, crises, output_path)
    
    # 过滤成功的指标
    successful_results = [r for r in results if r.get("status") == "success"]
    
    if not successful_results:
        print("❌ 没有成功的指标数据，无法生成长图")
        return
    
    # 计算子图布局
    n_indicators = len(successful_results)
    n_cols = 2  # 每行2个图，给更多空间显示详细信息
    n_rows = math.ceil(n_indicators / n_cols)
    
    # 创建大图
    fig_width = 20
    fig_height = n_rows * 6
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(fig_width, fig_height))
    fig.suptitle('宏观金融危机监察报告 - 详细图表', fontsize=18, fontweight='bold')
    
    # 确保axes是二维数组
    if n_rows == 1:
        axes = axes.reshape(1, -1)
    elif n_cols == 1:
        axes = axes.reshape(-1, 1)
    
    # 为每个指标创建详细子图
    for i, result in enumerate(successful_results):
        row = i // n_cols
        col = i % n_cols
        ax = axes[row, col]
        
        try:
            series_id = result["series_id"]
            indicator_name = result["indicator"]
            
            # 尝试从缓存获取数据
            if series_id in data_cache:
                ts = data_cache[series_id]
                
                # 绘制时间序列
                ax.plot(ts.index, ts.values, linewidth=1.5, alpha=0.8, label='数据')
                
                # 添加移动平均线
                if len(ts) > 12:
                    ma6 = ts.rolling(6).mean()
                    ma12 = ts.rolling(12).mean()
                    ax.plot(ma6.index, ma6.values, '--', alpha=0.7, label='6月均线')
                    ax.plot(ma12.index, ma12.values, ':', alpha=0.7, label='12月均线')
                
                # 标记最新值
                latest_date = ts.index[-1]
                latest_value = ts.iloc[-1]
                ax.scatter([latest_date], [latest_value], color='red', s=50, zorder=5)
                
                # 添加危机期阴影
                for crisis in crises:
                    try:
                        start_date = pd.to_datetime(crisis["start"])
                        end_date = pd.to_datetime(crisis["end"])
                        ax.axvspan(start_date, end_date, alpha=0.2, color='red', 
                                 label=crisis["name"] if crisis == crises[0] else "")
                    except:
                        continue
                
                # 设置标题和标签
                risk_score = result["risk_score"]
                risk_level = result["risk_level"]
                color = 'green' if risk_score < 50 else 'orange' if risk_score < 80 else 'red'
                
                ax.set_title(f"{indicator_name}\n{risk_level} ({risk_score:.1f}/100)", 
                           fontsize=11, color=color, fontweight='bold')
                ax.set_ylabel(result.get("unit", ""), fontsize=9)
                ax.grid(True, alpha=0.3)
                ax.legend(fontsize=8)
                
                # 格式化x轴
                ax.xaxis.set_major_formatter(DateFormatter("%Y-%m"))
                fig.autofmt_xdate()
                
            else:
                # 没有数据时显示信息
                ax.text(0.5, 0.5, 
                       f"{indicator_name}\n"
                       f"无数据可用",
                       ha='center', va='center',
                       fontsize=10, color='gray')
                ax.axis('off')
            
        except Exception as e:
            ax.text(0.5, 0.5, f"图表生成失败\n{str(e)}", 
                   ha='center', va='center', fontsize=8, color='red')
            ax.axis('off')
    
    # 隐藏多余的子图
    for i in range(n_indicators, n_rows * n_cols):
        row = i // n_cols
        col = i % n_cols
        axes[row, col].axis('off')
    
    plt.tight_layout()
    fig.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"📊 详细长图已生成: {output_path}")
