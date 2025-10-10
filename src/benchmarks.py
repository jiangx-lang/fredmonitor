# src/benchmarks.py - 基准分位计算
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
import yaml

def load_crisis_periods(crisis_yaml_path: str) -> List[Tuple[str, str]]:
    """
    加载危机期间配置
    
    Args:
        crisis_yaml_path: 危机期间配置文件路径
    
    Returns:
        危机期间列表 [(start_date, end_date), ...]
    """
    with open(crisis_yaml_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    crisis_periods = []
    for crisis in config.get('crises', []):
        start_date = crisis.get('start')
        end_date = crisis.get('end')
        if start_date and end_date:
            crisis_periods.append((start_date, end_date))
    
    return crisis_periods

def create_crisis_mask(series: pd.Series, crisis_periods: List[Tuple[str, str]]) -> pd.Series:
    """
    创建危机期间掩码
    
    Args:
        series: 时间序列
        crisis_periods: 危机期间列表
    
    Returns:
        布尔掩码，True表示危机期间
    """
    mask = pd.Series(False, index=series.index)
    
    for start_date, end_date in crisis_periods:
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        
        # 创建危机期间掩码
        crisis_mask = (series.index >= start_dt) & (series.index <= end_dt)
        mask |= crisis_mask
    
    return mask

def calculate_benchmarks(series: pd.Series, crisis_periods: List[Tuple[str, str]], 
                        compare_to: str) -> Dict[str, float]:
    """
    计算基准分位数
    
    Args:
        series: 时间序列
        crisis_periods: 危机期间列表
        compare_to: 比较基准 ('noncrisis_p25', 'crisis_median', etc.)
    
    Returns:
        基准值字典
    """
    if series.empty:
        return {}
    
    # 创建危机和非危机掩码
    crisis_mask = create_crisis_mask(series, crisis_periods)
    noncrisis_mask = ~crisis_mask
    
    benchmarks = {}
    
    # 非危机期间分位数
    if noncrisis_mask.any():
        noncrisis_data = series[noncrisis_mask].dropna()
        if len(noncrisis_data) > 0:
            benchmarks['noncrisis_p25'] = noncrisis_data.quantile(0.25)
            benchmarks['noncrisis_p35'] = noncrisis_data.quantile(0.35)
            benchmarks['noncrisis_p50'] = noncrisis_data.quantile(0.50)
            benchmarks['noncrisis_p65'] = noncrisis_data.quantile(0.65)
            benchmarks['noncrisis_p75'] = noncrisis_data.quantile(0.75)
            benchmarks['noncrisis_p90'] = noncrisis_data.quantile(0.90)
            benchmarks['noncrisis_median'] = noncrisis_data.median()
    
    # 危机期间分位数
    if crisis_mask.any():
        crisis_data = series[crisis_mask].dropna()
        if len(crisis_data) > 0:
            benchmarks['crisis_p25'] = crisis_data.quantile(0.25)
            benchmarks['crisis_p50'] = crisis_data.quantile(0.50)
            benchmarks['crisis_median'] = crisis_data.median()
    
    # 全样本分位数
    all_data = series.dropna()
    if len(all_data) > 0:
        benchmarks['all_p25'] = all_data.quantile(0.25)
        benchmarks['all_p50'] = all_data.quantile(0.50)
        benchmarks['all_p75'] = all_data.quantile(0.75)
        benchmarks['all_median'] = all_data.median()
    
    return benchmarks

def get_benchmark_value(benchmarks: Dict[str, float], compare_to: str) -> float:
    """
    获取指定的基准值
    
    Args:
        benchmarks: 基准值字典
        compare_to: 比较基准
    
    Returns:
        基准值
    """
    if compare_to in benchmarks:
        return benchmarks[compare_to]
    
    # 回退到默认值
    if 'noncrisis_p50' in benchmarks:
        return benchmarks['noncrisis_p50']
    elif 'all_p50' in benchmarks:
        return benchmarks['all_p50']
    else:
        return 0.0

def validate_benchmark_consistency(series: pd.Series, benchmarks: Dict[str, float]) -> List[str]:
    """
    验证基准值的一致性
    
    Args:
        series: 时间序列
        benchmarks: 基准值字典
    
    Returns:
        不一致问题列表
    """
    issues = []
    
    if series.empty:
        issues.append("时间序列为空")
        return issues
    
    # 检查基准值是否在合理范围内
    series_min = series.min()
    series_max = series.max()
    
    for benchmark_name, benchmark_value in benchmarks.items():
        if benchmark_value < series_min or benchmark_value > series_max:
            issues.append(f"{benchmark_name} ({benchmark_value:.2f}) 超出序列范围 [{series_min:.2f}, {series_max:.2f}]")
    
    # 检查分位数的单调性
    p_values = ['p25', 'p50', 'p75']
    for p in p_values:
        noncrisis_key = f'noncrisis_{p}'
        crisis_key = f'crisis_{p}'
        
        if noncrisis_key in benchmarks and crisis_key in benchmarks:
            if benchmarks[noncrisis_key] > benchmarks[crisis_key]:
                issues.append(f"非危机{p} ({benchmarks[noncrisis_key]:.2f}) > 危机{p} ({benchmarks[crisis_key]:.2f})")
    
    return issues
