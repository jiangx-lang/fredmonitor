#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V5.0: 生成宏观风险打分历史时间序列
从 crisis_monitor.py 提取核心逻辑，计算过去20年的每日/每月宏观风险评分
"""

import os
import sys
import pathlib
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# 工程路径
BASE = pathlib.Path(__file__).parent
sys.path.insert(0, str(BASE))

# 导入 crisis_monitor 的核心函数
try:
    from crisis_monitor import (
        load_yaml_config,
        fetch_series,
        compose_series,
        transform_series,
        calculate_benchmark_corrected,
        calculate_risk_score_simple,
        calculate_real_fred_scores
    )
except ImportError:
    print("❌ 无法导入 crisis_monitor 模块")
    sys.exit(1)

def calculate_historical_macro_scores(
    start_date="2005-01-01",
    end_date=None,
    frequency="daily",
    output_path=None
):
    """
    V5.0: 计算历史宏观风险评分时间序列
    
    Parameters:
    -----------
    start_date : str
        开始日期（格式：YYYY-MM-DD）
    end_date : str or None
        结束日期，None 表示使用今天
    frequency : str
        'daily' 或 'monthly'，输出频率
    output_path : str or None
        输出文件路径，None 表示使用默认路径
    
    Returns:
    --------
    pd.DataFrame
        包含 Date, Macro_Score, Macro_Risk_Level 的 DataFrame
    """
    print("=" * 80)
    print("V5.0: 生成宏观风险打分历史时间序列")
    print("=" * 80)
    
    # 加载配置
    config_path = BASE / "config" / "crisis_indicators.yaml"
    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        return None
    
    config = load_yaml_config(config_path)
    indicators = config.get('indicators', [])
    scoring_config = config.get('scoring', {})
    
    # 加载危机期间配置
    crisis_config_path = BASE / "config" / "crisis_periods.yaml"
    crisis_config = load_yaml_config(crisis_config_path)
    crisis_periods = crisis_config.get('crises', [])
    
    print(f"📊 加载了 {len(indicators)} 个指标")
    print(f"📅 计算期间: {start_date} 至 {end_date or datetime.now().strftime('%Y-%m-%d')}")
    print(f"📈 输出频率: {frequency}")
    
    # 确定日期范围
    start_dt = pd.Timestamp(start_date)
    end_dt = pd.Timestamp(end_date) if end_date else pd.Timestamp.now()
    
    # 生成日期序列
    if frequency == "monthly":
        # 月度：每月最后一天
        date_range = pd.date_range(start=start_dt, end=end_dt, freq='ME')
    else:
        # 日度：每个交易日（近似为工作日）
        date_range = pd.date_range(start=start_dt, end=end_dt, freq='B')  # B = Business day
    
    print(f"📅 将计算 {len(date_range)} 个日期的评分")
    
    # 预先加载所有指标的历史数据
    print("\n📥 预加载所有指标的历史数据...")
    indicator_series = {}
    
    for indicator in indicators:
        series_id = indicator.get('series_id') or indicator.get('id')
        if not series_id:
            continue
        
        try:
            # 尝试合成序列
            ts = compose_series(series_id)
            if ts is None or ts.empty:
                ts = fetch_series(series_id)
            
            if ts is not None and not ts.empty:
                # 应用变换
                ts_trans = transform_series(series_id, ts, indicator)
                ts_trans = ts_trans.dropna()
                
                if not ts_trans.empty:
                    indicator_series[series_id] = {
                        'series': ts_trans,
                        'indicator': indicator
                    }
                    print(f"  ✅ {series_id}: {len(ts_trans)} 个数据点，日期范围 {ts_trans.index[0]} 至 {ts_trans.index[-1]}")
        except Exception as e:
            print(f"  ⚠️ {series_id}: 加载失败 - {e}")
            continue
    
    print(f"\n✅ 成功加载 {len(indicator_series)} 个指标的历史数据")
    
    # 计算每个日期的评分
    print("\n🧮 开始计算历史评分...")
    results = []
    
    # 获取分组权重（用于加权计算）
    group_weights = {}
    for indicator in indicators:
        group = indicator.get('group', 'unknown')
        weight = indicator.get('weight', 0)
        if group not in group_weights:
            group_weights[group] = []
        group_weights[group].append(weight)
    
    # 归一化分组权重
    total_group_weight = sum(sum(weights) for weights in group_weights.values())
    if total_group_weight > 0:
        for group in group_weights:
            group_weights[group] = sum(group_weights[group]) / total_group_weight
    else:
        # 平均分配
        num_groups = len(group_weights)
        for group in group_weights:
            group_weights[group] = 1.0 / num_groups if num_groups > 0 else 0
    
    # 遍历每个日期
    for i, current_date in enumerate(date_range):
        if (i + 1) % 100 == 0:
            print(f"  进度: {i+1}/{len(date_range)} ({100*(i+1)/len(date_range):.1f}%)")
        
        # 计算该日期的评分
        date_scores = {}
        date_group_scores = {}
        
        for series_id, data in indicator_series.items():
            indicator = data['indicator']
            ts_trans = data['series']
            group = indicator.get('group', 'unknown')
            
            # 获取该日期或之前的最新值
            available_data = ts_trans[ts_trans.index <= current_date]
            if available_data.empty:
                continue
            
            current_value = float(available_data.iloc[-1])
            
            # 计算基准值（使用到当前日期的历史数据）
            historical_data = ts_trans[ts_trans.index <= current_date]
            if historical_data.empty or len(historical_data) < 24:
                continue
            
            try:
                benchmark_value = calculate_benchmark_corrected(
                    series_id, indicator, historical_data, crisis_periods
                )
                
                # 计算风险评分
                risk_score = calculate_risk_score_simple(
                    current_value, benchmark_value, indicator, historical_data, scoring_config
                )
                
                # 收集分组分数
                if group not in date_group_scores:
                    date_group_scores[group] = []
                date_group_scores[group].append(risk_score)
                
            except Exception as e:
                continue
        
        # 计算加权总分
        total_score = 0
        for group, scores in date_group_scores.items():
            if scores:
                avg_score = np.mean(scores)
                weight = group_weights.get(group, 0)
                total_score += avg_score * weight
        
        # 确定风险等级
        if total_score >= 80:
            risk_level = "极高风险"
        elif total_score >= 60:
            risk_level = "偏高风险"
        elif total_score >= 40:
            risk_level = "中等风险"
        else:
            risk_level = "低风险"
        
        results.append({
            'Date': current_date,
            'Macro_Score': round(total_score, 2),
            'Macro_Risk_Level': risk_level
        })
    
    # 创建 DataFrame
    df = pd.DataFrame(results)
    
    # 前向填充缺失值（因为某些日期可能没有足够的数据）
    df['Macro_Score'] = df['Macro_Score'].ffill()
    df = df.dropna(subset=['Macro_Score'])
    
    # 保存到 CSV
    if output_path is None:
        output_path = BASE / "data" / "macro_history.csv"
    
    output_path = pathlib.Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(output_path, index=False)
    print(f"\n✅ 历史评分已保存: {output_path}")
    print(f"   - 总记录数: {len(df)}")
    print(f"   - 日期范围: {df['Date'].min()} 至 {df['Date'].max()}")
    print(f"   - 平均评分: {df['Macro_Score'].mean():.2f}")
    print(f"   - 评分范围: {df['Macro_Score'].min():.2f} - {df['Macro_Score'].max():.2f}")
    
    return df

if __name__ == "__main__":
    # 计算过去20年的历史数据
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=20*365)).strftime("%Y-%m-%d")
    
    # 生成月度数据（更快，适合历史分析）
    df = calculate_historical_macro_scores(
        start_date=start_date,
        end_date=end_date,
        frequency="monthly",  # 可以改为 "daily" 生成日度数据
        output_path=None  # 使用默认路径
    )
    
    if df is not None:
        print("\n✅ 历史宏观评分生成完成！")
    else:
        print("\n❌ 历史宏观评分生成失败")

