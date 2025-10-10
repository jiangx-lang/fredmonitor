"""
VIX波动率风险因子
"""

import pandas as pd
import numpy as np
import os
from typing import Dict, Any
from factors.base_factor import BaseFactor


class VIXRiskFactor(BaseFactor):
    """VIX波动率风险因子"""
    
    def __init__(self, factor_config: Dict[str, Any]):
        super().__init__(factor_config)
        self.id = "VIX_RISK"
        self.name = "VIX波动率风险"
        self.series_id = "VIXCLS"
        self.description = "VIX波动率指数，衡量市场恐慌情绪"
        self.group = "市场情绪"
        self.weight = 0.15
        self.units = "指数"
    
    def fetch(self) -> pd.DataFrame:
        """获取VIX数据"""
        try:
            # 尝试从本地缓存读取
            cache_path = os.path.join("data", "fred", "series", self.series_id, "raw.csv")
            if os.path.exists(cache_path):
                df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
                df = df.reset_index()
                df.columns = ['date', 'value']
                return df.dropna()
            else:
                # 返回空DataFrame，让GUI通过FRED API获取
                return pd.DataFrame(columns=['date', 'value'])
        except Exception as e:
            print(f"获取VIX数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算VIX相关指标"""
        if df.empty:
            return {}
        
        values = df['value'].astype(float).dropna()
        if len(values) == 0:
            return {}
        
        metrics = {
            'current_value': float(values.iloc[-1]),
            'mean_20d': self.calculate_moving_average(df, 20),
            'mean_50d': self.calculate_moving_average(df, 50),
            'std_20d': self.calculate_volatility(df, 20),
            'percentile_rank': self.get_percentile_rank(df, 252),
            'trend_5d': self.calculate_trend(df, 5),
            'trend_20d': self.calculate_trend(df, 20),
            'max_20d': float(values.tail(20).max()) if len(values) >= 20 else float(values.max()),
            'min_20d': float(values.tail(20).min()) if len(values) >= 20 else float(values.min()),
            'range_20d': 0,
        }
        
        # 计算20日范围
        if metrics['max_20d'] > 0 and metrics['min_20d'] > 0:
            metrics['range_20d'] = metrics['max_20d'] - metrics['min_20d']
        
        return metrics
    
    def score(self, metrics: Dict[str, Any], settings: Dict[str, Any]) -> float:
        """计算VIX风险评分"""
        if not metrics:
            return 0
        
        current_value = metrics.get('current_value', 0)
        percentile_rank = metrics.get('percentile_rank', 0)
        
        # VIX特殊评分逻辑
        if current_value >= 40:
            return 95  # 极高风险
        elif current_value >= 30:
            return 80  # 高风险
        elif current_value >= 25:
            return 65  # 中高风险
        elif current_value >= 20:
            return 50  # 中等风险
        elif current_value >= 15:
            return 35  # 低风险
        elif current_value >= 10:
            return 20  # 极低风险
        else:
            return 10  # 异常低值