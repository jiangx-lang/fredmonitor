#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据质量检查脚本
"""

import pandas as pd
import numpy as np
import pathlib

def check_data_quality():
    """检查数据质量"""
    # 读取最新的CSV报告
    csv_file = pathlib.Path("outputs/crisis_monitor/crisis_report_20250918_195352.csv")
    if not csv_file.exists():
        print("❌ 找不到最新的CSV报告文件")
        return
    
    df = pd.read_csv(csv_file)
    
    print('🔍 数据质量检查报告')
    print('=' * 50)

    # 1. 检查过期数据
    stale_data = df[df['stale'] == True]
    print(f'📅 过期数据 ({len(stale_data)} 个):')
    for _, row in stale_data.iterrows():
        print(f'  - {row["indicator"]} ({row["series_id"]}): 最新 {row["last_observation"]}')

    # 2. 检查失败的数据
    failed_data = df[df['status'] != 'success']
    print(f'\n❌ 失败数据 ({len(failed_data)} 个):')
    for _, row in failed_data.iterrows():
        print(f'  - {row["indicator"]} ({row["series_id"]}): {row.get("error_message", "未知错误")}')

    # 3. 检查异常值
    print(f'\n📊 数据统计:')
    print(f'  - 总指标数: {len(df)}')
    print(f'  - 成功指标: {len(df[df["status"] == "success"])}')
    print(f'  - 失败指标: {len(df[df["status"] == "error"])}')
    print(f'  - 跳过指标: {len(df[df["status"] == "skipped"])}')

    # 4. 检查风险评分分布
    success_data = df[df['status'] == 'success']
    if len(success_data) > 0:
        risk_scores = success_data['risk_score'].dropna()
        print(f'\n🎯 风险评分分布:')
        print(f'  - 平均分: {risk_scores.mean():.1f}')
        print(f'  - 中位数: {risk_scores.median():.1f}')
        print(f'  - 标准差: {risk_scores.std():.1f}')
        print(f'  - 最小值: {risk_scores.min():.1f}')
        print(f'  - 最大值: {risk_scores.max():.1f}')
        
        # 风险等级分布
        risk_levels = success_data['risk_level'].value_counts()
        print(f'\n🚨 风险等级分布:')
        for level, count in risk_levels.items():
            print(f'  - {level}: {count} 个')

    # 5. 检查数据点数量
    print(f'\n📈 数据点数量统计:')
    data_points = success_data['data_points'].dropna()
    if len(data_points) > 0:
        print(f'  - 平均数据点: {data_points.mean():.0f}')
        print(f'  - 最少数据点: {data_points.min()}')
        print(f'  - 最多数据点: {data_points.max()}')
        
        # 找出数据点较少的指标
        low_data = success_data[success_data['data_points'] < 100]
        if len(low_data) > 0:
            print(f'\n⚠️ 数据点较少的指标 (<100):')
            for _, row in low_data.iterrows():
                print(f'  - {row["indicator"]}: {row["data_points"]} 个数据点')

    # 6. 检查危机期间使用情况
    print(f'\n📅 危机期间使用统计:')
    crisis_periods = success_data['crisis_periods_used'].dropna()
    if len(crisis_periods) > 0:
        print(f'  - 平均使用危机期间: {crisis_periods.mean():.1f}')
        print(f'  - 最少危机期间: {crisis_periods.min()}')
        print(f'  - 最多危机期间: {crisis_periods.max()}')
        
        # 找出危机期间使用较少的指标
        low_crisis = success_data[success_data['crisis_periods_used'] < 10]
        if len(low_crisis) > 0:
            print(f'\n⚠️ 危机期间使用较少的指标 (<10):')
            for _, row in low_crisis.iterrows():
                print(f'  - {row["indicator"]}: {row["crisis_periods_used"]} 个危机期间')

    # 7. 检查异常的风险评分
    print(f'\n🔍 异常风险评分检查:')
    extreme_scores = success_data[(success_data['risk_score'] >= 95) | (success_data['risk_score'] <= 5)]
    if len(extreme_scores) > 0:
        print(f'  极高/极低风险评分 (≥95 或 ≤5):')
        for _, row in extreme_scores.iterrows():
            print(f'  - {row["indicator"]}: {row["risk_score"]:.1f} ({row["risk_level"]})')

    # 8. 检查基准值异常
    print(f'\n📊 基准值异常检查:')
    benchmark_issues = success_data[
        (success_data['benchmark_value'].isna()) | 
        (success_data['benchmark_value'] == 0) |
        (abs(success_data['benchmark_value']) > 1e6)
    ]
    if len(benchmark_issues) > 0:
        print(f'  基准值异常:')
        for _, row in benchmark_issues.iterrows():
            print(f'  - {row["indicator"]}: 基准值 {row["benchmark_value"]}')

    print(f'\n✅ 数据质量检查完成')

if __name__ == "__main__":
    check_data_quality()
