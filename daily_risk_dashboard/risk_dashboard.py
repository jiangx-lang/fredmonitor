#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FRED 高频风险敏感监控系统（牛市逃顶信号）
- 每日生成灵敏度高的市场情绪与风险面板
- 输出: risk_dashboard_YYYYMMDD.png + JSON
"""

import os
import sys
import json
import yaml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from pathlib import Path
import warnings

# 抑制警告
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

# 工程路径
BASE = Path(__file__).parent
PARENT_BASE = BASE.parent
sys.path.insert(0, str(PARENT_BASE))

# 导入公共模块
try:
    from scripts.fred_http import series_observations
    from scripts.clean_utils import parse_numeric_series
    print("✅ 成功导入FRED模块")
except ImportError as e:
    print(f"⚠️ 导入FRED模块失败: {e}")
    print("将使用模拟数据")

# 配置路径
CONFIG_PATH = BASE / "config" / "risk_dashboard.yaml"
OUTPUT_DIR = BASE / "outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def load_config():
    """加载配置文件"""
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 配置文件加载失败: {e}")
        return None

def fetch_series(series_id):
    """获取FRED数据序列"""
    try:
        # 优先从本地缓存读取
        cache_path = PARENT_BASE / "data" / "fred" / "series" / series_id / "raw.csv"
        if cache_path.exists():
            df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            if not df.empty:
                return parse_numeric_series(df.iloc[:, 0]).dropna()
        
        # 如果本地没有，尝试API获取
        print(f"🌐 从API获取数据: {series_id}")
        data = series_observations(series_id)
        if data and 'observations' in data:
            observations = data.get('observations', [])
            if observations:
                df = pd.DataFrame(observations)
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date')
                ts = parse_numeric_series(df['value']).dropna()
                
                # 保存到缓存
                try:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    ts.to_frame(series_id).to_csv(cache_path)
                except Exception:
                    pass
                
                return ts
        
        print(f"⚠️ 无法获取数据: {series_id}")
        return pd.Series(dtype='float64')
        
    except Exception as e:
        print(f"❌ 获取数据失败 {series_id}: {e}")
        return pd.Series(dtype='float64')

def compute_percentile(ts, value, lookback_years=5):
    """计算历史分位数"""
    if ts.empty or pd.isna(value):
        return 0.5
    
    # 使用最近N年的数据
    lookback_days = lookback_years * 252  # 交易日
    sample = ts.dropna().tail(lookback_days)
    
    if len(sample) < 50:  # 数据不足
        return 0.5
    
    return (sample <= value).mean()

def compute_momentum_score(ts, days=[1, 5]):
    """计算动量评分"""
    if ts.empty or len(ts) < max(days) + 1:
        return 0
    
    momentum_scores = []
    for d in days:
        if len(ts) > d:
            change = abs(ts.iloc[-1] - ts.iloc[-1-d])
            # 计算变化量的历史分位数
            changes = ts.diff().abs().dropna()
            if len(changes) > 50:
                pct = (changes <= change).mean()
                momentum_scores.append(pct)
    
    return max(momentum_scores) if momentum_scores else 0

def calculate_indicator_score(series_id, indicator_config, lookback_years=5):
    """计算单个指标的风险评分"""
    try:
        # 获取数据
        ts = fetch_series(series_id)
        if ts.empty:
            return None
        
        current_value = ts.iloc[-1]
        if pd.isna(current_value):
            return None
        
        # 计算基础分位数
        pct = compute_percentile(ts, current_value, lookback_years)
        
        # 方向调整
        direction = indicator_config.get('direction', 'up_is_risk')
        if direction == 'down_is_risk':
            pct = 1 - pct
        elif direction == 'tilt_change':
            # 对于变化敏感的指标，使用变化量
            if len(ts) > 1:
                change = current_value - ts.iloc[-2]
                change_pct = compute_percentile(ts.diff().dropna(), change, lookback_years)
                pct = max(change_pct, 1 - change_pct)  # 取绝对值变化的分位数
        
        # 基础评分
        base_score = pct * 100
        
        # 动量加分
        momentum_score = compute_momentum_score(ts) * 5  # 最多加5分
        
        # 最终评分
        final_score = min(100, base_score + momentum_score)
        
        return {
            'series_id': series_id,
            'label': indicator_config.get('label', series_id),
            'current_value': current_value,
            'percentile': pct,
            'base_score': base_score,
            'momentum_score': momentum_score,
            'final_score': final_score,
            'last_date': str(ts.index[-1].date()),
            'direction': direction
        }
        
    except Exception as e:
        print(f"❌ 计算指标评分失败 {series_id}: {e}")
        return None

def calculate_bucket_score(indicators_results, bucket_config):
    """计算分组评分"""
    if not indicators_results:
        return 0
    
    scores = [r['final_score'] for r in indicators_results if r is not None]
    if not scores:
        return 0
    
    # 检查同组共振
    high_risk_count = sum(1 for s in scores if s >= 90)
    co_movement_bonus = 0
    if high_risk_count >= 2:
        co_movement_bonus = bucket_config.get('co_move_bonus_per_bucket', 5)
    
    bucket_score = np.mean(scores) + co_movement_bonus
    return min(100, bucket_score)

def generate_risk_dashboard_image(results, total_score, output_dir):
    """生成风险面板图片"""
    try:
        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial']
        plt.rcParams['axes.unicode_minus'] = False
        
        # 创建图表
        fig = plt.figure(figsize=(16, 12))
        gs = fig.add_gridspec(4, 3, height_ratios=[1, 2, 2, 1], hspace=0.3, wspace=0.3)
        
        # 标题
        ax_title = fig.add_subplot(gs[0, :])
        ax_title.text(0.5, 0.5, f'🚨 FRED 高频风险监控面板\n{datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")}', 
                     ha='center', va='center', fontsize=20, fontweight='bold')
        ax_title.set_xlim(0, 1)
        ax_title.set_ylim(0, 1)
        ax_title.axis('off')
        
        # 总体风险评分
        ax_score = fig.add_subplot(gs[1, 0])
        risk_color = '#d32f2f' if total_score >= 80 else '#f57c00' if total_score >= 65 else '#388e3c' if total_score >= 35 else '#1976d2'
        ax_score.pie([total_score, 100-total_score], colors=[risk_color, '#e0e0e0'], startangle=90)
        ax_score.text(0, 0, f'{total_score:.1f}', ha='center', va='center', fontsize=24, fontweight='bold')
        ax_score.set_title('总体风险评分', fontsize=14, fontweight='bold')
        
        # 风险等级
        ax_level = fig.add_subplot(gs[1, 1])
        risk_levels = ['极低风险', '低风险', '中风险', '高风险']
        risk_colors = ['#1976d2', '#388e3c', '#f57c00', '#d32f2f']
        
        if total_score >= 80:
            current_level = 3
        elif total_score >= 65:
            current_level = 2
        elif total_score >= 35:
            current_level = 1
        else:
            current_level = 0
        
        for i, (level, color) in enumerate(zip(risk_levels, risk_colors)):
            alpha = 1.0 if i == current_level else 0.3
            ax_level.barh(i, 100, color=color, alpha=alpha, height=0.6)
            ax_level.text(50, i, level, ha='center', va='center', fontweight='bold', fontsize=12)
        
        ax_level.set_xlim(0, 100)
        ax_level.set_ylim(-0.5, 3.5)
        ax_level.set_yticks(range(len(risk_levels)))
        ax_level.set_yticklabels(risk_levels)
        ax_level.set_title('风险等级', fontsize=14, fontweight='bold')
        ax_level.grid(True, alpha=0.3)
        
        # 触发统计
        ax_stats = fig.add_subplot(gs[1, 2])
        high_risk_count = sum(1 for r in results if r and r['final_score'] >= 80)
        medium_risk_count = sum(1 for r in results if r and 65 <= r['final_score'] < 80)
        total_indicators = len([r for r in results if r is not None])
        
        stats_text = f"触发统计\n\n高风险: {high_risk_count}\n中风险: {medium_risk_count}\n总计: {total_indicators}"
        ax_stats.text(0.5, 0.5, stats_text, ha='center', va='center', fontsize=12, 
                     bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.7))
        ax_stats.set_xlim(0, 1)
        ax_stats.set_ylim(0, 1)
        ax_stats.axis('off')
        
        # 指标详情表格
        ax_table = fig.add_subplot(gs[2, :])
        
        # 准备表格数据
        table_data = []
        for result in results:
            if result:
                risk_level = "🔴" if result['final_score'] >= 80 else "🟡" if result['final_score'] >= 65 else "🟢" if result['final_score'] >= 35 else "🔵"
                table_data.append([
                    result['label'],
                    f"{result['current_value']:.4f}",
                    f"{result['percentile']:.1%}",
                    f"{result['final_score']:.1f}",
                    risk_level
                ])
        
        if table_data:
            table = ax_table.table(cellText=table_data,
                                 colLabels=['指标', '当前值', '历史分位', '风险评分', '等级'],
                                 cellLoc='center',
                                 loc='center',
                                 colWidths=[0.25, 0.15, 0.15, 0.15, 0.1])
            table.auto_set_font_size(False)
            table.set_fontsize(10)
            table.scale(1, 2)
            
            # 设置表格样式
            for i in range(len(table_data)):
                score = float(table_data[i][3])
                if score >= 80:
                    color = '#ffcdd2'
                elif score >= 65:
                    color = '#fff3e0'
                elif score >= 35:
                    color = '#e8f5e8'
                else:
                    color = '#e3f2fd'
                
                for j in range(5):
                    table[(i+1, j)].set_facecolor(color)
        
        ax_table.axis('off')
        ax_table.set_title('指标详情', fontsize=14, fontweight='bold', pad=20)
        
        # 底部信息
        ax_footer = fig.add_subplot(gs[3, :])
        footer_text = f"数据来源: FRED | 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 仅供参考，不构成投资建议"
        ax_footer.text(0.5, 0.5, footer_text, ha='center', va='center', fontsize=10, style='italic')
        ax_footer.set_xlim(0, 1)
        ax_footer.set_ylim(0, 1)
        ax_footer.axis('off')
        
        # 保存图片
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        image_path = output_dir / f"risk_dashboard_{timestamp}.png"
        plt.savefig(image_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        
        print(f"✅ 风险面板图片已保存: {image_path}")
        return str(image_path)
        
    except Exception as e:
        print(f"❌ 生成风险面板图片失败: {e}")
        return None

def main():
    """主函数"""
    print("🚨 启动FRED高频风险监控系统...")
    print("=" * 60)
    
    # 加载配置
    config = load_config()
    if not config:
        return
    
    risk_config = config.get('risk_dashboard', {})
    buckets = risk_config.get('buckets', [])
    
    print(f"📊 加载了 {len(buckets)} 个风险分组")
    
    # 计算所有指标
    all_results = []
    bucket_scores = {}
    total_weighted_score = 0
    
    for bucket in buckets:
        bucket_name = bucket['name']
        bucket_weight = bucket['weight']
        indicators = bucket.get('indicators', [])
        
        print(f"\n📈 处理分组: {bucket_name} (权重: {bucket_weight:.1%})")
        
        bucket_results = []
        for indicator in indicators:
            series_id = indicator['id']
            print(f"  🔍 处理指标: {series_id}")
            
            result = calculate_indicator_score(series_id, indicator)
            if result:
                bucket_results.append(result)
                all_results.append(result)
                print(f"    ✅ {result['label']}: {result['final_score']:.1f}分")
            else:
                print(f"    ❌ {series_id}: 数据获取失败")
        
        # 计算分组评分
        bucket_score = calculate_bucket_score(bucket_results, bucket)
        bucket_scores[bucket_name] = {
            'score': bucket_score,
            'weight': bucket_weight,
            'count': len(bucket_results)
        }
        
        total_weighted_score += bucket_score * bucket_weight
        print(f"  📊 {bucket_name} 分组评分: {bucket_score:.1f}")
    
    # 生成风险面板图片
    print(f"\n🎨 生成风险面板图片...")
    image_path = generate_risk_dashboard_image(all_results, total_weighted_score, OUTPUT_DIR)
    
    # 保存JSON数据
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUTPUT_DIR / f"risk_dashboard_{timestamp}.json"
    
    json_data = {
        'timestamp': timestamp,
        'total_score': total_weighted_score,
        'bucket_scores': bucket_scores,
        'indicators': all_results,
        'summary': {
            'total_indicators': len(all_results),
            'high_risk_count': len([r for r in all_results if r['final_score'] >= 80]),
            'medium_risk_count': len([r for r in all_results if 65 <= r['final_score'] < 80]),
            'low_risk_count': len([r for r in all_results if r['final_score'] < 65])
        }
    }
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ JSON数据已保存: {json_path}")
    
    # 输出总结
    print("\n" + "=" * 60)
    print("📊 风险监控总结")
    print("=" * 60)
    print(f"🎯 总体风险评分: {total_weighted_score:.1f}/100")
    
    if total_weighted_score >= 80:
        risk_level = "🔴 高风险"
    elif total_weighted_score >= 65:
        risk_level = "🟡 中风险"
    elif total_weighted_score >= 35:
        risk_level = "🟢 低风险"
    else:
        risk_level = "🔵 极低风险"
    
    print(f"📈 风险等级: {risk_level}")
    print(f"📋 处理指标数: {len(all_results)}")
    print(f"🔴 高风险指标: {json_data['summary']['high_risk_count']}")
    print(f"🟡 中风险指标: {json_data['summary']['medium_risk_count']}")
    print(f"🟢 低风险指标: {json_data['summary']['low_risk_count']}")
    
    if image_path:
        print(f"🖼️ 风险面板图片: {image_path}")
    
    print("=" * 60)
    print("✅ FRED高频风险监控完成!")

if __name__ == "__main__":
    main()
