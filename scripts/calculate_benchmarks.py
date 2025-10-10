#!/usr/bin/env python3
"""
计算正确的基准值脚本
修正基准值计算，使用YoY百分比而非绝对水平
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
import sys
sys.path.append('.')
from scripts.fred_http import series_observations

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_crisis_periods():
    """
    加载危机期间定义
    """
    crisis_periods = {
        'GFC_2008': ('2007-12-01', '2009-06-01'),
        'COVID_2020': ('2020-02-01', '2020-04-01'),
        'DOTCOM_2001': ('2001-03-01', '2001-11-01'),
        'SAVINGS_CRISIS_1990': ('1990-07-01', '1991-03-01'),
        'OIL_CRISIS_1973': ('1973-11-01', '1975-03-01'),
        'OIL_CRISIS_1980': ('1980-01-01', '1980-07-01'),
        'VOLCKER_1981': ('1981-07-01', '1982-11-01'),
        'BLACK_MONDAY_1987': ('1987-10-01', '1987-12-01'),
        'ASIAN_CRISIS_1997': ('1997-07-01', '1998-01-01'),
        'LTCM_1998': ('1998-08-01', '1998-10-01'),
        'Y2K_2000': ('2000-03-01', '2001-01-01'),
        'EUROPEAN_CRISIS_2011': ('2011-07-01', '2012-01-01'),
        'TAPER_TANTRUM_2013': ('2013-05-01', '2013-09-01'),
        'CHINA_CRISIS_2015': ('2015-08-01', '2016-02-01'),
        'COVID_2020_EXTENDED': ('2020-02-01', '2020-12-01')
    }
    return crisis_periods

def create_crisis_mask(dates, crisis_periods):
    """
    创建危机期间掩码
    """
    crisis_mask = pd.Series(False, index=dates)
    
    for crisis_name, (start_date, end_date) in crisis_periods.items():
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        crisis_mask |= (dates >= start) & (dates <= end)
    
    return crisis_mask

def calculate_yoy_percentage(df, freq):
    """
    计算YoY百分比
    """
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    if freq == 'M':
        shift_periods = 12
    elif freq == 'Q':
        shift_periods = 4
    elif freq == 'W':
        shift_periods = 52
    else:
        logger.warning(f"未知频率: {freq}")
        return df
    
    # 计算YoY百分比
    df['yoy_pct'] = df['value'].pct_change(shift_periods) * 100
    return df

def calculate_benchmark_for_indicator(series_id, freq, compare_to, crisis_periods):
    """
    为单个指标计算正确的基准值
    """
    logger.info(f"计算 {series_id} 的基准值...")
    
    try:
        # 获取数据
        data = series_observations(series_id)
        if not data or 'observations' not in data:
            logger.error(f"无法获取 {series_id} 数据")
            return None
        
        # 转换为DataFrame
        df = pd.DataFrame(data['observations'])
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df = df.dropna()
        
        # 计算YoY百分比
        df_yoy = calculate_yoy_percentage(df, freq)
        df_yoy = df_yoy.dropna()
        
        if df_yoy.empty:
            logger.error(f"{series_id} YoY计算后无数据")
            return None
        
        # 创建危机掩码
        crisis_mask = create_crisis_mask(df_yoy['date'], crisis_periods)
        
        # 根据compare_to计算基准
        if compare_to.startswith('crisis_'):
            # 使用危机期间数据
            crisis_data = df_yoy.loc[crisis_mask, 'yoy_pct'].dropna()
            if crisis_data.empty:
                logger.warning(f"{series_id} 危机期间无数据，使用全样本")
                crisis_data = df_yoy['yoy_pct'].dropna()
        elif compare_to.startswith('noncrisis_'):
            # 使用非危机期间数据
            noncrisis_data = df_yoy.loc[~crisis_mask, 'yoy_pct'].dropna()
            if noncrisis_data.empty:
                logger.warning(f"{series_id} 非危机期间无数据，使用全样本")
                noncrisis_data = df_yoy['yoy_pct'].dropna()
        else:
            # 使用全样本
            crisis_data = df_yoy['yoy_pct'].dropna()
        
        # 计算分位数
        if 'median' in compare_to:
            benchmark = crisis_data.median() if compare_to.startswith('crisis_') else noncrisis_data.median()
        elif 'p25' in compare_to:
            benchmark = crisis_data.quantile(0.25) if compare_to.startswith('crisis_') else noncrisis_data.quantile(0.25)
        elif 'p75' in compare_to:
            benchmark = crisis_data.quantile(0.75) if compare_to.startswith('crisis_') else noncrisis_data.quantile(0.75)
        elif 'p90' in compare_to:
            benchmark = crisis_data.quantile(0.90) if compare_to.startswith('crisis_') else noncrisis_data.quantile(0.90)
        elif 'p65' in compare_to:
            benchmark = crisis_data.quantile(0.65) if compare_to.startswith('crisis_') else noncrisis_data.quantile(0.65)
        else:
            benchmark = crisis_data.median() if compare_to.startswith('crisis_') else noncrisis_data.median()
        
        logger.info(f"✅ {series_id}: {compare_to} = {benchmark:.2f}%")
        return benchmark
        
    except Exception as e:
        logger.error(f"❌ {series_id} 基准计算失败: {e}")
        return None

def main():
    """
    主函数：计算所有指标的基准值
    """
    print("=" * 60)
    print("基准值计算工具")
    print("=" * 60)
    
    # 加载危机期间定义
    crisis_periods = load_crisis_periods()
    
    # 定义需要计算基准的指标
    indicators = [
        ('GDP', 'Q', 'crisis_p25'),
        ('PAYEMS', 'M', 'crisis_p25'),
        ('INDPRO', 'M', 'crisis_p25'),
        ('NEWORDER', 'M', 'crisis_p25'),
        ('TOTLL', 'W', 'noncrisis_p75'),
        ('CSUSHPINSA', 'M', 'noncrisis_p90'),
        ('TOTALSA', 'M', 'noncrisis_p75'),
        ('NCBDBIQ027S', 'Q', 'noncrisis_p65'),
        ('WALCL', 'W', 'crisis_median'),
        ('DTWEXBGS', 'W', 'noncrisis_median'),
        ('MANEMP', 'M', 'crisis_p25'),
        ('PERMIT', 'M', 'crisis_p25'),
        ('TOTRESNS', 'M', 'noncrisis_p25'),
    ]
    
    benchmarks = {}
    
    for series_id, freq, compare_to in indicators:
        benchmark = calculate_benchmark_for_indicator(series_id, freq, compare_to, crisis_periods)
        if benchmark is not None:
            benchmarks[series_id] = {
                'benchmark': benchmark,
                'compare_to': compare_to,
                'freq': freq
            }
        print()  # 空行分隔
    
    # 保存基准值
    output_file = Path("data/benchmarks_corrected.json")
    import json
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(benchmarks, f, indent=2, ensure_ascii=False)
    
    print("=" * 60)
    print(f"基准值计算完成: {len(benchmarks)} 个指标")
    print(f"结果保存到: {output_file}")
    print("=" * 60)
    
    # 显示结果摘要
    print("\n📊 基准值摘要:")
    for series_id, data in benchmarks.items():
        print(f"  {series_id}: {data['compare_to']} = {data['benchmark']:.2f}%")

if __name__ == "__main__":
    main()
