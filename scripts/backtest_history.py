#!/usr/bin/env python3
"""
Project Time Machine: 历史回测脚本
使用 v2.2 System Tension 算法回测 2000 年至今的历史数据
验证是否能精准捕捉到 2000、2008、2020 三次大危机
"""

import os
import sys
import pathlib
import yaml
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from datetime import datetime
from typing import Dict, List, Optional

# 强制设置标准输出为utf-8
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 添加项目根目录到路径
BASE_DIR = pathlib.Path(__file__).parent.parent
sys.path.append(str(BASE_DIR))

# v2.2: 逻辑阵营映射（与 crisis_monitor.py 保持一致）
CATEGORY_MAPPING = {
    'State': [
        '权益周期监控 (Equity_Cycle)',
        '长期估值锚 (Secular_Valuation)',
        'real_economy',
        'consumers_leverage',
        'banking',
        '黄金见顶监控'
    ],
    'Trigger': [
        'liquidity',
        'core_warning',
        'recession_leading'
    ],
    'Constraint': [
        'monetary_policy',
        'inflation_expectations',
        'monitoring'
    ]
}

# 关键时刻标记
CRISIS_MOMENTS = [
    ('2000-03-10', '纳指崩盘前夕'),
    ('2007-10-11', '美股07年高点/次贷爆发'),
    ('2020-02-19', '疫情熔断前夜'),
    ('2021-12-31', '2022熊市前高')
]


def load_config() -> Dict:
    """加载配置文件"""
    config_path = BASE_DIR / "config" / "crisis_indicators.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def load_indicator_data(series_id: str, data_dir: pathlib.Path) -> Optional[pd.Series]:
    """
    加载单个指标的历史数据
    支持多种格式：date,value 或 索引为日期
    """
    # 尝试多个可能的路径
    possible_paths = [
        data_dir / "series" / f"{series_id}.csv",
        data_dir / "series" / f"{series_id}_YOY.csv",
        data_dir / "fred" / "series" / series_id / "raw.csv",
    ]
    
    for csv_path in possible_paths:
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                
                # 处理 date,value 格式
                if 'date' in df.columns and 'value' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                    ts = df['value'].dropna()
                # 处理索引为日期的格式
                elif len(df.columns) == 1:
                    df.index = pd.to_datetime(df.index)
                    ts = df.iloc[:, 0].dropna()
                else:
                    # 尝试第一列作为索引
                    df.index = pd.to_datetime(df.iloc[:, 0])
                    ts = df.iloc[:, 1].dropna()
                
                # 去除时区信息
                if ts.index.tz is not None:
                    ts.index = ts.index.tz_localize(None)
                
                return ts
            except Exception as e:
                print(f"⚠️ 读取 {series_id} 失败 ({csv_path}): {e}")
                continue
    
    return None


def load_market_data() -> Optional[pd.Series]:
    """加载 SPY 市场数据"""
    # 尝试多个可能的路径
    possible_paths = [
        BASE_DIR / "data" / "market" / "SPY.csv",
        pathlib.Path(r"D:\标普\data_clean\SPY.csv"),
    ]
    
    for csv_path in possible_paths:
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
                
                # 确定收盘价列
                close_col = None
                for col in ['close', 'Close', 'Adj Close', 'adj_close']:
                    if col in df.columns:
                        close_col = col
                        break
                
                if close_col is None:
                    print(f"⚠️ SPY 数据缺少收盘价列")
                    return None
                
                ts = df[close_col].dropna()
                
                # 去除时区信息
                if ts.index.tz is not None:
                    ts.index = ts.index.tz_localize(None)
                
                return ts
            except Exception as e:
                print(f"⚠️ 读取 SPY 失败 ({csv_path}): {e}")
                continue
    
    return None


def calculate_simple_score(current_value: float, ts: pd.Series, 
                          higher_is_risk: bool, compare_to: str) -> float:
    """
    简化的评分计算（用于回测）
    使用历史分位数作为基准
    """
    if ts is None or ts.empty or pd.isna(current_value):
        return 50.0
    
    # 解析 compare_to 字符串，获取分位数
    pct_map = {
        'crisis_median': 0.5,
        'crisis_p25': 0.25,
        'crisis_p75': 0.75,
        'noncrisis_median': 0.5,
        'noncrisis_p25': 0.25,
        'noncrisis_p75': 0.75,
        'noncrisis_p90': 0.90,
    }
    
    pct = pct_map.get(compare_to, 0.5)
    benchmark = ts.quantile(pct)
    
    if pd.isna(benchmark) or abs(benchmark) < 1e-10:
        return 50.0
    
    # 计算偏离度
    if higher_is_risk:
        # 高为险：当前值越高，风险越高
        if current_value >= benchmark:
            deviation = (current_value - benchmark) / abs(benchmark) * 100
        else:
            deviation = -(benchmark - current_value) / abs(benchmark) * 100
    else:
        # 低为险：当前值越低，风险越高
        if current_value <= benchmark:
            deviation = (benchmark - current_value) / abs(benchmark) * 100
        else:
            deviation = -(current_value - benchmark) / abs(benchmark) * 100
    
    # 风险评分：0-100
    risk_score = max(0, min(100, 50 + deviation))
    return risk_score


def calculate_weekly_tension(date: pd.Timestamp, all_data: pd.DataFrame, 
                            indicators_config: List[Dict]) -> Dict:
    """
    计算指定日期的 System Tension Index
    """
    # 获取该日期之前的所有历史数据（用于计算基准值）
    historical_data = all_data.loc[all_data.index <= date]
    
    if historical_data.empty:
        return {
            'state_score': 50.0,
            'trigger_score': 50.0,
            'tension_index': 25.0
        }
    
    # 获取该日期的当前值
    current_values = all_data.loc[date]
    
    # 按组分类计算得分
    group_scores = {}
    
    for indicator in indicators_config:
        series_id = indicator.get('id')
        if series_id not in current_values.index:
            continue
        
        group = indicator.get('group', 'unknown')
        if group not in group_scores:
            group_scores[group] = {'scores': [], 'weights': []}
        
        # 获取当前值
        current_value = current_values[series_id]
        if pd.isna(current_value):
            continue
        
        # 获取历史数据
        if series_id in historical_data.columns:
            ts = historical_data[series_id].dropna()
            if ts.empty:
                continue
            
            # 计算得分
            higher_is_risk = indicator.get('higher_is_risk', True)
            compare_to = indicator.get('compare_to', 'noncrisis_median')
            weight = indicator.get('weight', 0)
            
            score = calculate_simple_score(current_value, ts, higher_is_risk, compare_to)
            
            group_scores[group]['scores'].append(score)
            group_scores[group]['weights'].append(weight)
    
    # 计算各组的平均分
    final_group_scores = {}
    for group, data in group_scores.items():
        if data['scores']:
            avg_score = np.mean(data['scores'])
            total_weight = sum(data['weights'])
            final_group_scores[group] = {
                'score': avg_score,
                'weight': total_weight
            }
    
    # 计算 State 和 Trigger 得分
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
        weight = group_data.get('weight', 0)
        score = group_data.get('score', 0)
        score_state += score * weight
        total_state_weight += weight
    
    if total_state_weight > 0:
        score_state = score_state / total_state_weight
    else:
        score_state = 50.0
    
    score_trigger = 0.0
    total_trigger_weight = 0.0
    for group_data in trigger_groups:
        weight = group_data.get('weight', 0)
        score = group_data.get('score', 0)
        score_trigger += score * weight
        total_trigger_weight += weight
    
    if total_trigger_weight > 0:
        score_trigger = score_trigger / total_trigger_weight
    else:
        score_trigger = 50.0
    
    # v2.3: 计算系统张力指数（引入凸性算法）
    # 注意：这里只返回基础分数，动量计算在主循环中进行
    return {
        'state_score': score_state,
        'trigger_score': score_trigger,
        'tension_index': (score_state * score_trigger) / 100.0  # 临时值，将在主循环中重新计算
    }


def main():
    """主函数"""
    print("=" * 80)
    print("🚀 Project Time Machine: 历史回测脚本 (v2.3 Fragility Amplifier)")
    print("=" * 80)
    
    # 1. 加载配置
    print("\n[1/5] 加载配置文件...")
    config = load_config()
    indicators = config.get('indicators', [])
    print(f"✅ 加载了 {len(indicators)} 个指标配置")
    
    # 2. 加载所有指标数据
    print("\n[2/5] 加载历史数据...")
    data_dir = BASE_DIR / "data"
    all_series = {}
    
    for indicator in indicators:
        series_id = indicator.get('id')
        ts = load_indicator_data(series_id, data_dir)
        if ts is not None and not ts.empty:
            all_series[series_id] = ts
            print(f"  ✅ {series_id}: {len(ts)} 个数据点 ({ts.index.min()} 至 {ts.index.max()})")
        else:
            print(f"  ⚠️ {series_id}: 数据不可用")
    
    print(f"\n✅ 成功加载 {len(all_series)} 个指标的数据")
    
    # 3. 合并数据并重采样为周频
    print("\n[3/5] 合并数据并重采样为周频...")
    if not all_series:
        print("❌ 没有可用的数据，退出")
        return
    
    # 创建统一的 DataFrame
    all_data = pd.DataFrame(all_series)
    
    # 重采样为周频（周五）
    all_data_weekly = all_data.resample('W-FRI').last()
    
    # 使用前向填充处理缺失值
    all_data_weekly = all_data_weekly.ffill()
    
    # 过滤时间范围：2000-01-01 至今
    start_date = pd.Timestamp('2000-01-01')
    all_data_weekly = all_data_weekly.loc[all_data_weekly.index >= start_date]
    
    print(f"✅ 周频数据: {len(all_data_weekly)} 周 ({all_data_weekly.index.min()} 至 {all_data_weekly.index.max()})")
    
    # 4. 加载 SPY 数据
    print("\n[4/5] 加载 SPY 市场数据...")
    spy_data = load_market_data()
    if spy_data is not None:
        spy_weekly = spy_data.resample('W-FRI').last()
        spy_weekly = spy_weekly.loc[spy_weekly.index >= start_date]
        print(f"✅ SPY 周频数据: {len(spy_weekly)} 周")
    else:
        print("⚠️ SPY 数据不可用，将只显示 Tension Index")
        spy_weekly = None
    
    # 5. 计算每周的 System Tension Index (v2.3: 先计算基础分数，再计算动量)
    print("\n[5/6] 计算基础分数 (State & Trigger)...")
    base_results = []
    
    for date in all_data_weekly.index:
        tension = calculate_weekly_tension(date, all_data_weekly, indicators)
        
        base_results.append({
            'Date': date,
            'State_Score': tension['state_score'],
            'Trigger_Score': tension['trigger_score']
        })
        
        if len(base_results) % 100 == 0:
            print(f"  处理进度: {len(base_results)}/{len(all_data_weekly)} 周")
    
    base_df = pd.DataFrame(base_results)
    base_df = base_df.set_index('Date')
    
    # v2.3: 计算 Trigger Momentum (13周移动平均)
    print("\n[6/6] 计算 v2.3 凸性张力 (含动量)...")
    base_df['Trigger_Mean_13W'] = base_df['Trigger_Score'].rolling(window=13, min_periods=1).mean()
    base_df['Trigger_Momentum'] = base_df['Trigger_Score'] - base_df['Trigger_Mean_13W']
    # 前期数据不足13周时，用0填充动量
    base_df['Trigger_Momentum'] = base_df['Trigger_Momentum'].fillna(0.0)
    
    # v2.3: 计算最终张力（使用凸性算法）
    results = []
    for date in base_df.index:
        state = base_df.loc[date, 'State_Score']
        trigger = base_df.loc[date, 'Trigger_Score']
        momentum = base_df.loc[date, 'Trigger_Momentum']
        
        # === v2.3 核心逻辑 ===
        # 1. 基础张力 (Base Tension): 传统的 Level * Level
        raw_tension = (state * trigger) / 100.0
        
        # 2. 动量压力 (Momentum Stress): 专门捕捉"变化率"
        # 只有当环境在恶化 (Momentum > 0) 时才计算
        mom_risk = max(0.0, momentum)
        
        # 3. 凸性放大器 (Convexity Amplifier - 指数版)
        base_threshold = 40.0
        scale = 15.0
        max_mult = 8.0  # 稍微调高上限，允许在极端泡沫期报警
        
        if state > base_threshold:
            # 计算放大倍数（指数函数）
            x = (state - base_threshold) / scale
            convexity_factor = min(max_mult, 1.0 + (math.exp(x) - 1.0))
        else:
            convexity_factor = 1.0
        
        # 4. 最终合成 (Final Synthesis)
        # 总张力 = 基础张力 + (动量风险 * 放大倍数)
        index_tension = raw_tension + (mom_risk * convexity_factor)
        
        # 封顶
        index_tension = min(100.0, index_tension)
        
        spy_value = None
        if spy_weekly is not None and date in spy_weekly.index:
            spy_value = spy_weekly.loc[date]
        
        results.append({
            'Date': date,
            'SPY': spy_value,
            'State_Score': state,
            'Trigger_Score': trigger,
            'Trigger_Momentum': momentum,
            'Tension_Index': index_tension,
            'Convexity_Factor': convexity_factor
        })
        
        if len(results) % 100 == 0:
            print(f"  处理进度: {len(results)}/{len(base_df)} 周")
    
    results_df = pd.DataFrame(results)
    results_df = results_df.set_index('Date')
    
    print(f"✅ 计算完成: {len(results_df)} 周的数据")
    
    # 6. 保存结果
    output_dir = BASE_DIR / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    csv_path = output_dir / "history_tension.csv"
    results_df.to_csv(csv_path, encoding='utf-8')
    print(f"\n✅ 结果已保存: {csv_path}")
    
    # 7. 可视化
    print("\n[6/6] 生成可视化图表...")
    fig, ax1 = plt.subplots(figsize=(16, 10))
    
    # 左轴：SPY（对数刻度）
    if spy_weekly is not None:
        ax1.set_xlabel('日期', fontsize=12)
        ax1.set_ylabel('SPY (对数刻度)', color='black', fontsize=12)
        ax1.plot(results_df.index, results_df['SPY'], 'k-', linewidth=1.5, label='SPY')
        ax1.set_yscale('log')
        ax1.tick_params(axis='y', labelcolor='black')
        ax1.grid(True, alpha=0.3)
    
    # 右轴：System Tension Index
    ax2 = ax1.twinx()
    ax2.set_ylabel('System Tension Index', color='red', fontsize=12)
    ax2.fill_between(results_df.index, 0, results_df['Tension_Index'], 
                     alpha=0.3, color='red', label='Tension Index')
    ax2.plot(results_df.index, results_df['Tension_Index'], 'r-', linewidth=2, label='Tension Index')
    ax2.set_ylim(0, 100)
    ax2.tick_params(axis='y', labelcolor='red')
    
    # 标记关键时刻
    for date_str, label in CRISIS_MOMENTS:
        try:
            crisis_date = pd.Timestamp(date_str)
            if crisis_date in results_df.index:
                ax1.axvline(x=crisis_date, color='blue', linestyle='--', linewidth=1.5, alpha=0.7)
                ax1.text(crisis_date, ax1.get_ylim()[1] * 0.95, label, 
                        rotation=90, verticalalignment='top', fontsize=9, color='blue')
        except:
            pass
    
    # 设置标题和标签
    plt.title('Project Time Machine: System Tension Index 历史回测 (v2.3 Fragility Amplifier)', 
              fontsize=14, fontweight='bold', pad=20)
    
    # 添加图例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图片
    img_path = output_dir / "backtest_result.png"
    plt.savefig(img_path, dpi=300, bbox_inches='tight')
    print(f"✅ 图表已保存: {img_path}")
    
    # 显示关键时刻的 Tension Index
    print("\n📊 关键时刻的 System Tension Index:")
    print("-" * 80)
    for date_str, label in CRISIS_MOMENTS:
        try:
            crisis_date = pd.Timestamp(date_str)
            # 找到最接近的日期
            closest_date = results_df.index[results_df.index.get_indexer([crisis_date], method='nearest')[0]]
            tension = results_df.loc[closest_date, 'Tension_Index']
            state = results_df.loc[closest_date, 'State_Score']
            trigger = results_df.loc[closest_date, 'Trigger_Score']
            momentum = results_df.loc[closest_date, 'Trigger_Momentum']
            convexity = results_df.loc[closest_date, 'Convexity_Factor']
            print(f"{label} ({closest_date.strftime('%Y-%m-%d')}):")
            print(f"  State: {state:.1f}, Trigger: {trigger:.1f}, Momentum: {momentum:.1f}")
            print(f"  Convexity: {convexity:.2f}x, Tension: {tension:.1f}")
        except Exception as e:
            print(f"  ⚠️ {label}: 无法获取数据 ({e})")
    
    print("\n" + "=" * 80)
    print("✅ Project Time Machine 回测完成！")
    print("=" * 80)


if __name__ == "__main__":
    main()
