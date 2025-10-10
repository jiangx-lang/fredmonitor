# src/transforms.py - 数据变换处理
import pandas as pd
import numpy as np
from typing import Union

def apply_transform(series: pd.Series, transform_method: str, freq: str = 'D') -> pd.Series:
    """
    应用数据变换
    
    Args:
        series: 原始时间序列
        transform_method: 变换方法 ('level', 'yoy_pct', 'diff', 'zscore')
        freq: 数据频率 ('D', 'W', 'M', 'Q')
    
    Returns:
        变换后的时间序列
    """
    if series.empty:
        return series
    
    # 移除NaN值
    series = series.dropna()
    
    if transform_method == 'level':
        return series
    
    elif transform_method == 'yoy_pct':
        # 同比百分比变化
        if freq == 'D':
            shift_periods = 252  # 交易日
        elif freq == 'W':
            shift_periods = 52   # 周
        elif freq == 'M':
            shift_periods = 12   # 月
        elif freq == 'Q':
            shift_periods = 4    # 季度
        else:
            shift_periods = 12   # 默认月度
        
        if len(series) < shift_periods + 1:
            return pd.Series(dtype=float)
        
        yoy_pct = ((series / series.shift(shift_periods)) - 1) * 100
        return yoy_pct
    
    elif transform_method == 'diff':
        # 一阶差分
        return series.diff()
    
    elif transform_method == 'zscore':
        # Z-score标准化
        if len(series) < 2:
            return pd.Series(dtype=float)
        return (series - series.mean()) / series.std()
    
    else:
        raise ValueError(f"未知的变换方法: {transform_method}")

def resample_to_monthly(series: pd.Series, freq: str) -> pd.Series:
    """
    将序列重采样到月度频率
    
    Args:
        series: 原始时间序列
        freq: 原始频率
    
    Returns:
        月度重采样后的序列
    """
    if series.empty:
        return series
    
    if freq == 'D':
        # 日度数据 -> 月末
        return series.resample('ME').last()
    elif freq == 'W':
        # 周度数据 -> 月末
        return series.resample('ME').last()
    elif freq == 'M':
        # 月度数据 -> 保持不变
        return series
    elif freq == 'Q':
        # 季度数据 -> 月末
        return series.resample('ME').last()
    else:
        # 默认重采样到月末
        return series.resample('ME').last()

def validate_transform_consistency(series_id: str, name: str, transform: str) -> bool:
    """
    验证变换方法的一致性
    
    Args:
        series_id: 指标ID
        name: 指标名称
        transform: 变换方法
    
    Returns:
        是否一致
    """
    # 检查名称中是否包含YoY但变换方法不是yoy_pct
    if 'YoY' in name and transform != 'yoy_pct':
        print(f"⚠️ 警告: {series_id} 名称含YoY但变换方法为{transform}")
        return False
    
    # 检查名称中是否包含"同比"但变换方法不是yoy_pct
    if '同比' in name and transform != 'yoy_pct':
        print(f"⚠️ 警告: {series_id} 名称含同比但变换方法为{transform}")
        return False
    
    return True

def get_transform_hint(series_id: str, name: str) -> str:
    """
    根据指标名称建议变换方法
    
    Args:
        series_id: 指标ID
        name: 指标名称
    
    Returns:
        建议的变换方法
    """
    if 'YoY' in name or '同比' in name:
        return 'yoy_pct'
    elif '利差' in name or 'spread' in series_id.lower():
        return 'level'
    elif '利率' in name or 'rate' in series_id.lower():
        return 'level'
    elif '指数' in name or 'index' in series_id.lower():
        return 'level'
    else:
        return 'level'  # 默认水平值
