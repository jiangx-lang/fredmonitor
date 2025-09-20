#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据清洗工具函数
"""

import numpy as np
import pandas as pd
from typing import Union, List


def clean_value(x) -> float:
    """清洗单个数值，处理各种非标准格式"""
    if x is None:
        return np.nan
    
    y = str(x).strip()
    
    # 处理各种空值表示
    if y in {".", "", "NaN", "N.A.", "NA", "ND", "--"}:
        return np.nan
    
    # 移除逗号和百分号
    y = y.replace(",", "").replace("%", "")
    
    # 处理括号表示的负数 (1.23) -> -1.23
    if y.startswith("(") and y.endswith(")"):
        y = "-" + y[1:-1]
    
    try:
        return float(y)
    except:
        return np.nan


def parse_numeric_series(series: Union[pd.Series, List]) -> pd.Series:
    """解析数值序列，处理各种非标准格式"""
    if isinstance(series, list):
        series = pd.Series(series)
    
    # 应用清洗函数
    cleaned = series.apply(clean_value)
    
    # 替换无穷大值
    cleaned = cleaned.replace([np.inf, -np.inf], np.nan)
    
    return cleaned


def clean_dataframe(df: pd.DataFrame, value_columns: List[str] = None) -> pd.DataFrame:
    """清洗DataFrame中的数值列"""
    df = df.copy()
    
    if value_columns is None:
        # 自动检测数值列
        value_columns = df.select_dtypes(include=[object]).columns.tolist()
    
    for col in value_columns:
        if col in df.columns:
            df[col] = parse_numeric_series(df[col])
    
    return df
