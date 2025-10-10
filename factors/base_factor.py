"""
因子基类
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod


class BaseFactor(ABC):
    """因子基类"""
    
    def __init__(self, factor_config: Dict[str, Any]):
        """
        初始化因子
        
        Args:
            factor_config: 因子配置字典
        """
        self.id = factor_config.get('id', '')
        self.name = factor_config.get('name', '')
        self.series_id = factor_config.get('series_id', '')
        self.description = factor_config.get('description', '')
        self.group = factor_config.get('group', '')
        self.weight = factor_config.get('weight', 0.0)
        self.bands = factor_config.get('bands', [])
        self.higher_is_risk = factor_config.get('higher_is_risk', True)
        self.units = factor_config.get('units', '')
    
    @abstractmethod
    def fetch(self) -> pd.DataFrame:
        """
        获取数据
        
        Returns:
            DataFrame with columns ['date', 'value']
        """
        pass
    
    @abstractmethod
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        计算指标
        
        Args:
            df: 包含['date', 'value']列的DataFrame
            
        Returns:
            指标字典
        """
        pass
    
    def score(self, metrics: Dict[str, Any], settings: Dict[str, Any]) -> float:
        """
        计算风险评分
        
        Args:
            metrics: 指标字典
            settings: 系统设置
            
        Returns:
            风险评分 (0-100)
        """
        if not metrics:
            return 0
        
        current_value = metrics.get('current_value', 0)
        
        # 基于bands计算评分
        if not self.bands:
            return self._default_scoring(current_value)
        
        return self._band_based_scoring(current_value)
    
    def _default_scoring(self, current_value: float) -> float:
        """默认评分方法"""
        if self.higher_is_risk:
            # 正向评分：值越高风险越高
            if current_value >= 100:
                return 90
            elif current_value >= 50:
                return 70
            elif current_value >= 25:
                return 50
            elif current_value >= 10:
                return 30
            else:
                return 10
        else:
            # 反向评分：值越低风险越高
            if current_value <= -10:
                return 90
            elif current_value <= -5:
                return 70
            elif current_value <= 0:
                return 50
            elif current_value <= 5:
                return 30
            else:
                return 10
    
    def _band_based_scoring(self, current_value: float) -> float:
        """基于bands的评分方法"""
        bands = self.bands
        reverse = len(bands) > 3 and bands[3] == "reverse"
        
        if len(bands) < 2:
            return self._default_scoring(current_value)
        
        # 检查是否有reverse标记
        if reverse:
            # 反向评分
            if current_value <= bands[0]:
                return 90  # 高风险
            elif current_value <= bands[1]:
                return 70  # 中高风险
            elif current_value <= bands[2]:
                return 50  # 中等风险
            else:
                return 30  # 低风险
        else:
            # 正向评分
            if current_value >= bands[2]:
                return 90  # 高风险
            elif current_value >= bands[1]:
                return 70  # 中高风险
            elif current_value >= bands[0]:
                return 50  # 中等风险
            else:
                return 30  # 低风险
    
    def get_percentile_rank(self, df: pd.DataFrame, lookback_days: int = 252) -> float:
        """计算当前值的百分位排名"""
        if df.empty:
            return 0
        
        values = df['value'].astype(float).dropna()
        if len(values) < 2:
            return 0
        
        # 使用最近lookback_days的数据
        recent_values = values.tail(min(lookback_days, len(values)))
        current_value = values.iloc[-1]
        
        # 计算百分位排名
        percentile_rank = (recent_values <= current_value).mean() * 100
        
        if not self.higher_is_risk:
            # 反向因子：百分位排名也需要反向
            percentile_rank = 100 - percentile_rank
        
        return float(percentile_rank)
    
    def calculate_trend(self, df: pd.DataFrame, days: int = 5) -> float:
        """计算趋势变化率"""
        if df.empty or len(df) < days + 1:
            return 0
        
        values = df['value'].astype(float).dropna()
        if len(values) < days + 1:
            return 0
        
        current_value = values.iloc[-1]
        past_value = values.iloc[-(days + 1)]
        
        if past_value == 0:
            return 0
        
        return float((current_value - past_value) / past_value * 100)
    
    def calculate_volatility(self, df: pd.DataFrame, days: int = 20) -> float:
        """计算波动率"""
        if df.empty or len(df) < days:
            return 0
        
        values = df['value'].astype(float).dropna()
        if len(values) < days:
            return 0
        
        recent_values = values.tail(days)
        return float(recent_values.std())
    
    def calculate_moving_average(self, df: pd.DataFrame, days: int = 20) -> float:
        """计算移动平均"""
        if df.empty or len(df) < days:
            return 0
        
        values = df['value'].astype(float).dropna()
        if len(values) < days:
            return float(values.mean())
        
        recent_values = values.tail(days)
        return float(recent_values.mean())
    
    def get_latest_value(self, df: pd.DataFrame) -> float:
        """获取最新值"""
        if df.empty:
            return 0
        
        values = df['value'].astype(float).dropna()
        if len(values) == 0:
            return 0
        
        return float(values.iloc[-1])
    
    def get_data_range(self, df: pd.DataFrame) -> tuple:
        """获取数据范围"""
        if df.empty:
            return (None, None)
        
        dates = df['date']
        return (dates.min(), dates.max())
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """验证数据格式"""
        if df.empty:
            return False
        
        required_columns = ['date', 'value']
        if not all(col in df.columns for col in required_columns):
            return False
        
        # 检查是否有有效数据
        values = df['value'].astype(float).dropna()
        return len(values) > 0


# 为了向后兼容，添加别名
Factor = BaseFactor