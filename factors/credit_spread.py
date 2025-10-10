"""
信用利差因子
"""

import pandas as pd
import numpy as np
import os
from typing import Dict, Any
from factors.base_factor import BaseFactor


class CreditSpreadFactor(BaseFactor):
    """信用利差因子"""
    
    def __init__(self, factor_config: Dict[str, Any]):
        super().__init__(factor_config)
        self.id = "CREDIT_SPREAD"
        self.name = "信用利差"
        self.series_id = "BAMLH0A0HYM2"
        self.description = "高收益债券风险溢价"
        self.group = "信用风险"
        self.weight = 0.15
        self.units = "基点"
        self.higher_is_risk = True  # 正向因子：利差越高风险越高
    
    def fetch(self) -> pd.DataFrame:
        """获取信用利差数据"""
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
            print(f"获取信用利差数据失败: {e}")
            return pd.DataFrame(columns=['date', 'value'])
    
    def compute(self, df: pd.DataFrame) -> Dict[str, Any]:
        """计算信用利差相关指标"""
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
            'credit_stress_level': self._get_credit_stress_level(values.iloc[-1]),
        }
        
        # 计算20日范围
        if metrics['max_20d'] > 0 and metrics['min_20d'] > 0:
            metrics['range_20d'] = metrics['max_20d'] - metrics['min_20d']
        
        return metrics
    
    def _get_credit_stress_level(self, current_value: float) -> str:
        """获取信用压力水平"""
        if current_value >= 800:
            return "极高压力"
        elif current_value >= 600:
            return "高压力"
        elif current_value >= 400:
            return "中等压力"
        elif current_value >= 200:
            return "低压力"
        else:
            return "极低压力"
    
    def score(self, metrics: Dict[str, Any], settings: Dict[str, Any]) -> float:
        """计算信用利差风险评分"""
        if not metrics:
            return 0
        
        current_value = metrics.get('current_value', 0)
        credit_stress_level = metrics.get('credit_stress_level', '极低压力')
        
        # 信用利差特殊评分逻辑
        if current_value >= 1000:
            return 95  # 极高风险
        elif current_value >= 800:
            return 85  # 高风险
        elif current_value >= 600:
            return 70  # 中高风险
        elif current_value >= 400:
            return 50  # 中等风险
        elif current_value >= 200:
            return 30  # 低风险
        else:
            return 15  # 极低风险
